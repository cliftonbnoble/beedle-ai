// Legacy alias: no target/database/checkpoint defaults are retained.
import { main } from './recovery.mjs';
main().then(result=>console.log(JSON.stringify(result,null,2))).catch(error=>{console.error(error.message);process.exitCode=1;});
