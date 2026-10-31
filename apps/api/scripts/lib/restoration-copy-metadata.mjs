import {createHash} from 'node:crypto';

// The management API's HTTP ETag may describe a weak, encoded response. Pin the
// raw object ETag from its JSON metadata instead. These restoration copies are
// single-part puts of at most 1 MB; SHA-256 remains the source identity check.
export function verifiedRestorationCopyMetadata({metadata,key,bytes,ownerId,planSha256,sourceSha256,sourceBytes}){
 const fail=()=>{throw Error('restoration_copy_object_metadata_mismatch')};
 if(!metadata||typeof metadata!=='object'||Array.isArray(metadata)||typeof key!=='string'||!key||
   !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(ownerId)||!/^[a-f0-9]{64}$/.test(planSha256)||
   !(bytes instanceof Uint8Array)||!Number.isSafeInteger(sourceBytes)||sourceBytes<1||sourceBytes>1_000_000||
   bytes.byteLength!==sourceBytes||!/^[a-f0-9]{64}$/.test(sourceSha256)||
   createHash('sha256').update(bytes).digest('hex')!==sourceSha256)fail();
 if(metadata.key!==key||metadata.size!==sourceBytes||typeof metadata.etag!=='string'||
   !/^[a-f0-9]{32}$/.test(metadata.etag)||metadata.etag!==createHash('md5').update(bytes).digest('hex')||
   metadata.custom_metadata?.ownerid!==ownerId||metadata.custom_metadata.plansha256!==planSha256||
   metadata.custom_metadata.sourcesha256!==sourceSha256)fail();
 return {etag:metadata.etag,sourceSha256,sourceBytes};
}
