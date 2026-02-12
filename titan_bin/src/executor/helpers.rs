use crate::catalog::{SystemCatalog, update_pg_class_page_id};
use crate::errors::ExecutionError;
use crate::parser::LiteralValue;
use crate::types::Column;
use bedrock::PageId;
use bedrock::buffer_pool::BufferPoolManager;
use bedrock::transaction::{Snapshot, TransactionManager};
use bedrock::wal::WalManager;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub fn parse_tuple(tuple_data: &[u8], schema: &[Column]) -> HashMap<String, LiteralValue> {
    let mut offset = 0;
    let mut parsed_tuple = HashMap::new();

    for col in schema {
        if offset >= tuple_data.len() {
            break;
        }
        match col.type_id {
            16 => {
                let Some(byte) = tuple_data.get(offset) else {
                    break;
                };
                parsed_tuple.insert(col.name.clone(), LiteralValue::Bool(*byte != 0));
                offset += 1;
            }
            23 => {
                let Some(bytes_slice) = tuple_data.get(offset..offset + 4) else {
                    break;
                };
                let Some(bytes) = <[u8; 4]>::try_from(bytes_slice).ok() else {
                    break;
                };
                let val = i32::from_be_bytes(bytes);
                parsed_tuple.insert(col.name.clone(), LiteralValue::Number(val.to_string()));
                offset += 4;
            }
            25 => {
                let Some(len_slice) = tuple_data.get(offset..offset + 4) else {
                    break;
                };
                let Some(len_bytes) = <[u8; 4]>::try_from(len_slice).ok() else {
                    break;
                };
                let len = u32::from_be_bytes(len_bytes) as usize;
                offset += 4;
                let Some(value_slice) = tuple_data.get(offset..offset + len) else {
                    break;
                };
                let val = String::from_utf8_lossy(value_slice);
                parsed_tuple.insert(col.name.clone(), LiteralValue::String(val.into_owned()));
                offset += len;
            }
            1082 => {
                let Some(bytes_slice) = tuple_data.get(offset..offset + 4) else {
                    break;
                };
                let Some(bytes) = <[u8; 4]>::try_from(bytes_slice).ok() else {
                    break;
                };
                let days = i32::from_be_bytes(bytes);
                let Some(epoch) = NaiveDate::from_ymd_opt(2000, 1, 1) else {
                    break;
                };
                let date = epoch + chrono::Duration::days(days as i64);
                parsed_tuple.insert(
                    col.name.clone(),
                    LiteralValue::Date(date.format("%Y-%m-%d").to_string()),
                );
                offset += 4;
            }
            _ => {}
        }
    }
    parsed_tuple
}

pub(crate) fn row_vec_to_map(
    row_vec: &[String],
    columns: &[Column],
    table_name: Option<&str>,
) -> HashMap<String, LiteralValue> {
    let mut map = HashMap::new();
    for (i, col) in columns.iter().enumerate() {
        let Some(val_str) = row_vec.get(i) else {
            break;
        };
        let literal = match col.type_id {
            16 => LiteralValue::Bool(val_str == "t"),
            23 => LiteralValue::Number(val_str.clone()),
            25 => LiteralValue::String(val_str.clone()),
            1082 => LiteralValue::Date(val_str.clone()),
            _ => LiteralValue::String(val_str.clone()),
        };
        if let Some(table) = table_name {
            map.insert(format!("{}.{}", table, col.name), literal.clone());
        }
        map.insert(col.name.clone(), literal);
    }
    map
}

#[derive(Clone)]
pub(crate) struct IntIndexInfo {
    pub(crate) column_name: String,
    pub(crate) index_oid: u32,
    pub(crate) root_page_id: PageId,
}

pub(crate) fn extract_i32_key(
    row: &HashMap<String, LiteralValue>,
    column_name: &str,
) -> Option<i32> {
    match row.get(column_name) {
        Some(LiteralValue::Number(v)) => v.parse::<i32>().ok(),
        _ => None,
    }
}

pub(crate) fn load_convention_int_indexes(
    bpm: &Arc<BufferPoolManager>,
    tx_id: u32,
    snapshot: &Snapshot,
    system_catalog: &Arc<Mutex<SystemCatalog>>,
    schema: &[Column],
) -> Result<Vec<IntIndexInfo>, ExecutionError> {
    let mut indexes = Vec::new();
    let mut catalog = system_catalog
        .lock()
        .map_err(|_| ExecutionError::GenericError("system catalog lock poisoned".to_string()))?;

    for column in schema {
        if column.type_id != 23 {
            continue;
        }

        let index_name = format!("idx_{}", column.name);
        if let Some((index_oid, root_page_id)) =
            catalog.find_table(&index_name, bpm, tx_id, snapshot)?
        {
            indexes.push(IntIndexInfo {
                column_name: column.name.clone(),
                index_oid,
                root_page_id,
            });
        }
    }

    Ok(indexes)
}

pub(crate) fn update_index_root_if_needed(
    bpm: &Arc<BufferPoolManager>,
    tm: &Arc<TransactionManager>,
    wm: &Arc<Mutex<WalManager>>,
    tx_id: u32,
    snapshot: &Snapshot,
    index: &mut IntIndexInfo,
    new_root_page_id: PageId,
) -> Result<(), ExecutionError> {
    if new_root_page_id != index.root_page_id {
        update_pg_class_page_id(
            bpm,
            tm,
            wm,
            tx_id,
            snapshot,
            index.index_oid,
            new_root_page_id,
        )?;
        index.root_page_id = new_root_page_id;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_tuple, row_vec_to_map};
    use crate::types::Column;

    #[test]
    fn parse_tuple_handles_truncated_int_without_panicking() {
        let schema = vec![Column {
            name: "id".to_string(),
            type_id: 23,
        }];
        let parsed = parse_tuple(&[0x00, 0x01], &schema);
        assert!(parsed.is_empty());
    }

    #[test]
    fn row_vec_to_map_ignores_missing_cells_without_panicking() {
        let columns = vec![
            Column {
                name: "id".to_string(),
                type_id: 23,
            },
            Column {
                name: "name".to_string(),
                type_id: 25,
            },
        ];
        let row = vec!["1".to_string()];
        let map = row_vec_to_map(&row, &columns, None);

        assert_eq!(map.get("id").map(|v| v.to_string()), Some("1".to_string()));
        assert!(!map.contains_key("name"));
    }
}
