export const queryCases = [
  ['repair notice', {}], ['breach of quiet enjoyment', {judgeName:'Florence Quill'}],
  ['tenant roof leak', {indexCodes:['100'],indexCodeOperator:'and'}], ['37.8', {ordinanceSection:'37.8'}],
  ['owner move in', {}], ['section 8 unlawful detainer', {}], ['mold', {}], ['heat', {}],
  ['garage space', {}], ['storage', {}], ['T-12345', {fromDate:'2024-01-01',toDate:'2024-12-31'}],
  ['Florence Quill', {}], ['repair notice', {documentIds:['alpha','beta']}]
];
export const rows = [
 {chunkId:'alpha/p1',documentId:'alpha',documentRevision:3,retrievalRevision:2,title:'Repair proceeding',citation:'T-12345',authorName:'Florence Quill',decisionDate:'2024-02-29',fileType:'decision_docx',sourceFileRef:'source/alpha.md',sourceLink:'https://example.invalid/alpha',sectionLabel:'Conclusions of Law',paragraphAnchor:'p1',citationAnchor:'T-12345#p1',chunkText:'The tenant gave written repair notice about a roof leak. The Board considered the repair notice and the complete record under Ordinance 37.8.',createdAt:'2024-02-29',indexCodesJson:'["100"]',rulesSectionsJson:'["1.2"]',ordinanceSectionsJson:'["37.8"]',isTrustedTier:1,searchableAt:'2024-03-01',orderRank:1},
 {chunkId:'beta/p2',documentId:'beta',documentRevision:1,retrievalRevision:1,title:'Housing services proceeding',citation:'T-43210',authorName:null,decisionDate:null,fileType:'decision_docx',sourceFileRef:'source/beta.md',sourceLink:'https://example.invalid/beta',sectionLabel:'Findings of Fact',paragraphAnchor:'p2',citationAnchor:'T-43210#p2',chunkText:'The tenant reported mold, no heat, a garage storage dispute and an unlawful detainer. The owner discussed an owner move in. The record addresses housing services and quiet enjoyment.',createdAt:'2024-03-01',indexCodesJson:'[]',rulesSectionsJson:'[]',ordinanceSectionsJson:'[]',isTrustedTier:0,orderRank:2}
];
