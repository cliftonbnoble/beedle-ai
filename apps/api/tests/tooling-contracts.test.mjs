import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import ts from 'typescript';
const root = fileURLToPath(new URL('../', import.meta.url)), require = createRequire(import.meta.url);
const { ESLint } = require('eslint');
test('typed lint rejects floating/misused promises, unchecked any and double assertions', async () => {
    const eslint = new ESLint({ cwd: root });
    const [result] = await eslint.lintText(`declare function task():Promise<void>;
 task();
 if(Promise.resolve(true)) console.log('unsafe condition');
 const unsafe:any=JSON.parse('{}');console.log(unsafe.field);
 const unknownValue:unknown={};const forced=unknownValue as unknown as {ok:boolean};console.log(forced.ok);`, { filePath: path.join(root, 'src/lib/provider-operation.ts') });
    const ids = new Set(result.messages.map(x => x.ruleId));
    for (const rule of ['@typescript-eslint/no-floating-promises', '@typescript-eslint/no-misused-promises', '@typescript-eslint/no-explicit-any', '@typescript-eslint/no-unsafe-member-access', '@typescript-eslint/no-unsafe-type-assertion', 'no-restricted-syntax'])
        assert.ok(ids.has(rule), rule + ': ' + JSON.stringify(result.messages));
});
test('typed lint accepts awaited work, unknown JSON handoff and context-owned background work', async () => {
    const eslint = new ESLint({ cwd: root });
    const [result] = await eslint.lintText(`declare function task():Promise<void>;
 export async function safe(ctx:ExecutionContext){const value:unknown=JSON.parse('{}');await task();await Promise.all([task(),task()]);ctx.waitUntil(task());return value;}`, { filePath: path.join(root, 'src/lib/provider-operation.ts') });
    assert.deepEqual(result.messages, []);
});
test('renaming a generated D1 binding fails application typechecking instead of falling back to a handwritten binding', () => {
    const config = ts.readConfigFile(path.join(root, 'tsconfig.json'), ts.sys.readFile), parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, root), host = ts.createCompilerHost(parsed.options), original = host.getSourceFile.bind(host);
    let mutated = false;
    host.getSourceFile = (file, ...args) => {
        const node = original(file, ...args);
        if (node && path.normalize(file) === path.join(root, 'src/worker-configuration.d.ts')) {
            const text = node.text.replace(/\bDB: D1Database;/g, 'MISNAMED_DB: D1Database;');
            assert.notEqual(text, node.text);
            mutated = true;
            return ts.createSourceFile(file, text, parsed.options.target, true);
        }
        return node;
    };
    const program = ts.createProgram(parsed.fileNames, parsed.options, host), errors = program.getSemanticDiagnostics();
    assert.ok(mutated);
    assert.ok(errors.some(x => ts.flattenDiagnosticMessageText(x.messageText, ' ').includes("Property 'DB' does not exist")));
});
test('generated declarations contain only configured string variables and portable module paths', () => {
    const text = fs.readFileSync(path.join(root, 'src/worker-configuration.d.ts'), 'utf8'), config = JSON.parse(fs.readFileSync(path.join(root, 'wrangler.json'), 'utf8'));
    const allowed = new Set([config, ...Object.values(config.env || {})].flatMap(env => Object.keys(env.vars || {})));
    const ast = ts.createSourceFile('bindings.d.ts', text, ts.ScriptTarget.Latest, true);
    let checked = 0;
    for (const namespace of ast.statements) {
        if (!ts.isModuleDeclaration(namespace) || namespace.name.text !== 'Cloudflare' || !namespace.body || !ts.isModuleBlock(namespace.body))
            continue;
        for (const declaration of namespace.body.statements) {
            if (!ts.isInterfaceDeclaration(declaration) || declaration.name.text !== 'Env')
                continue;
            for (const member of declaration.members)
                if (ts.isPropertySignature(member) && member.type?.kind === ts.SyntaxKind.StringKeyword) {
                    assert.ok(allowed.has(member.name.getText(ast)), 'Unexpected local variable in generated bindings');
                    checked++;
                }
        }
    }
    assert.ok(checked > 0);
    assert.match(text, /interface WorkerBindings extends Cloudflare.Env/);
    assert.doesNotMatch(text, /\/Users\//);
});
