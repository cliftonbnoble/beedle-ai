export const spreadsheetCsvPolicy='spreadsheet-text-1';
/** CSV quoting protects field boundaries; a visible prefix separately marks
 * potentially executable TEXT as literal. Actual finite numbers stay numeric. */
export function spreadsheetCsvCell(value){
 let text=String(value??'');let protectedText=false;
 if(!(typeof value==='number'&&Number.isFinite(value))){
  const prefix=text.match(/^[\p{White_Space}\p{Cf}\u0000-\u001f\u007f-\u009f]*/u)[0];
  const first=text.slice(prefix.length).normalize('NFKC')[0];
  if(['=','+','-','@'].includes(first)||/[\p{Cf}\u0000-\u001f\u007f-\u009f]/u.test(prefix)){text="[text] "+text;protectedText=true;}
 }
 return protectedText||/[",;\t\r\n]/.test(text)?'"'+text.replaceAll('"','""')+'"':text;
}
/** Bounded RFC-style CSV reader. Keep presentation prefixes literal: removing
 * one cannot distinguish original text from the export's protection. */
export function readCsvRecords(input){
 if(typeof input!=='string'||input.length>32*1024*1024)throw Error('CSV input exceeds the supported limit');
 const text=input.startsWith('\uFEFF')?input.slice(1):input,records=[];
 let cells=[],cell='',state='start',line=1,rowLine=1;
 const field=()=>{cells.push(cell);cell='';if(cells.length>200)throw Error('CSV has too many columns');};
 const row=()=>{field();records.push({line:rowLine,cells});cells=[];rowLine=line+1;if(records.length>100001)throw Error('CSV has too many records');};
 for(let i=0;i<text.length;i++){
  const ch=text[i];
  if(state==='quoted'){
   if(ch==='"'){if(text[i+1]==='"'){cell+='"';i++;}else state='closed';}
   else{cell+=ch;if(ch==='\n'||ch==='\r'&&text[i+1]!=='\n')line++;}
  }else if(ch===','){field();state='start';}
  else if(ch==='\r'||ch==='\n'){row();if(ch==='\r'&&text[i+1]==='\n')i++;line++;state='start';}
  else if(state==='closed')throw Error(`CSV has characters after a closing quote at line ${line}`);
  else if(ch==='"'){if(state!=='start')throw Error(`CSV has a quote inside an unquoted field at line ${line}`);state='quoted';}
  else{cell+=ch;state='plain';}
  if(cell.length>1024*1024)throw Error('CSV field exceeds the supported limit');
 }
 if(state==='quoted')throw Error('CSV has an unterminated quoted field');
 if(cell.length||cells.length||state!=='start')row();
 return records;
}
