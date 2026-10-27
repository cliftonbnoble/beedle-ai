import tseslint from 'typescript-eslint';
import { fileURLToPath } from 'node:url';
// A focused typed baseline for ingress, external payloads and durable async work.
// Add a boundary here when changing it; no formatting or legacy ranking rewrite is implied.
export const boundaryFiles = [
    'src/index.ts', 'src/lib/types.ts', 'src/lib/provider-operation.ts', 'src/lib/stored-json.ts',
    'src/services/assistant-chat.ts', 'src/services/activation-contract.ts',
    'src/services/retrieval-activation.ts', 'src/services/embeddings.ts',
    'src/services/vector-jobs.ts', 'src/services/maintenance-jobs.ts',
    'src/services/ingestion-requests.ts', 'src/services/maintenance-contract.ts'
];
export default [{
        files: boundaryFiles,
        languageOptions: { parser: tseslint.parser, parserOptions: { projectService: true, tsconfigRootDir: fileURLToPath(new URL('.', import.meta.url)) } },
        plugins: { '@typescript-eslint': tseslint.plugin },
        rules: {
            '@typescript-eslint/no-floating-promises': ['error', { ignoreVoid: false, checkThenables: true }],
            '@typescript-eslint/no-misused-promises': 'error',
            '@typescript-eslint/await-thenable': 'error',
            '@typescript-eslint/no-explicit-any': 'error',
            '@typescript-eslint/no-unsafe-assignment': 'error',
            '@typescript-eslint/no-unsafe-argument': 'error',
            'no-restricted-syntax': ['error', { selector: 'TSAsExpression[expression.type="TSAsExpression"]', message: 'Do not double-assert external values; validate and narrow them at the boundary.' }],
            '@typescript-eslint/no-unsafe-return': 'error',
            '@typescript-eslint/no-unsafe-call': 'error',
            '@typescript-eslint/no-unsafe-member-access': 'error'
        }
    },{
        files:['src/lib/provider-operation.ts','src/lib/stored-json.ts','src/services/embeddings.ts','src/services/activation-contract.ts','src/services/retrieval-activation.ts','src/services/maintenance-contract.ts','src/services/ingestion-requests.ts'],
        rules:{'@typescript-eslint/no-unsafe-type-assertion':'error'}
    }];
