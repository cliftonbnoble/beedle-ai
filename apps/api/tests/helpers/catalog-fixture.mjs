export function catalogPayload(label='Current') {
  return {source_trace:{index_codes:'reviewed-index.json',ordinance:'reviewed-ordinance.json',rules:'reviewed-rules.json'},
    index_codes:[{code_identifier:'100',description:`${label} repairs`,linked_rules_sections:['1.2'],linked_ordinance_sections:['37.8']}],
    rules_sections:[{section_number:'1.2',body_text:`${label} rule requires review of the complete record.`}],
    ordinance_sections:[{section_number:'37.8',body_text:`${label} ordinance describes the repair proceeding.`}],crosswalk:[]};
}
