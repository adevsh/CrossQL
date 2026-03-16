import { invoke } from '@tauri-apps/api/core';
import { pipelineStore } from './pipelineStore.svelte';

function isFileSourceType(nodeType: string): boolean {
  return nodeType === 'file' || nodeType === 'csv_source' || nodeType === 'parquet_source';
}

function applySchemaMap(fields: Array<{ name: string; dtype: string }>, cfg: any) {
  const map = new Map<string, string>(fields.map((f) => [f.name, f.dtype]));
  const cols: any[] = cfg?.columns ?? [];
  for (const c of cols) {
    const source = typeof c?.source === 'string' ? c.source.trim() : '';
    if (!source) continue;
    const sourceDtype = map.get(source);
    if (!sourceDtype) continue;
    const rename = typeof c?.rename === 'string' ? c.rename.trim() : '';
    const cast = typeof c?.cast === 'string' ? c.cast.trim() : '';
    const targetName = rename || source;
    const targetDtype = cast ? cast : sourceDtype;
    if (targetName !== source) map.delete(source);
    map.set(targetName, targetDtype);
  }
  return Array.from(map.entries()).map(([name, dtype]) => ({ name, dtype }));
}

async function fetchSchemaForNode(nodeId: string, depth = 0): Promise<Array<{ name: string; dtype: string }>> {
  if (depth > 20) throw new Error('Schema resolution exceeded max depth');
  const { nodes, edges } = pipelineStore;
  const node = nodes.find((n: any) => n.id === nodeId);
  if (!node) throw new Error('Node not found');

  if (node.type === 'schema_map') {
    const incomingEdges = edges.filter((e: any) => e.target === node.id);
    if (incomingEdges.length !== 1) throw new Error('Schema Map must have exactly one incoming edge');
    const upstreamId = incomingEdges[0].source;
    const upstreamSchema = await fetchSchemaForNode(upstreamId, depth + 1);
    return applySchemaMap(upstreamSchema, node.data?.config);
  }

  if (node.type !== 'postgres' && node.type !== 'mysql' && node.type !== 'mongodb' && node.type !== 'cassandra' && !isFileSourceType(node.type)) {
    throw new Error('Schema preview is only available for source and Schema Map nodes');
  }

  if (!node.data?.config) throw new Error('Missing node config');

  if (isFileSourceType(node.type)) {
    const path = node.data.config?.path ?? '';
    if (!path) throw new Error('File path not configured');
    const result = await invoke('get_file_schema', { path });
    return (result as any[])?.map((x: any) => ({ name: x.name, dtype: x.dtype })) ?? [];
  }

  const command =
    node.type === 'postgres'
      ? 'preview_postgres_schema'
      : node.type === 'mysql'
        ? 'preview_mysql_schema'
        : node.type === 'mongodb'
          ? 'preview_mongodb_schema'
          : 'preview_cassandra_schema';
  const result = await invoke(command, { source: node.data.config });
  return (result as any[])?.map((x: any) => ({ name: x.name, dtype: x.dtype })) ?? [];
}

function createSchemaStore() {
  let schemaState = $state<'idle' | 'loading' | 'ready' | 'error'>('idle');
  let schemaError = $state<string | null>(null);
  let schemaFields = $state<Array<{ name: string; dtype: string }>>([]);

  let previewState = $state<'idle' | 'loading' | 'ready' | 'error'>('idle');
  let previewError = $state<string | null>(null);
  let previewColumns = $state<string[]>([]);
  let previewRows = $state<any[]>([]);

  async function loadSchema() {
    schemaState = 'idle';
    schemaError = null;
    schemaFields = [];

    const selectedNodeId = pipelineStore.selectedNodeId;
    if (!selectedNodeId) return;
    const node = pipelineStore.nodes.find((n: any) => n.id === selectedNodeId);
    if (!node) return;
    if (node.type !== 'postgres' && node.type !== 'mysql' && node.type !== 'mongodb' && node.type !== 'cassandra' && !isFileSourceType(node.type) && node.type !== 'schema_map') return;

    schemaState = 'loading';
    try {
      schemaFields = await fetchSchemaForNode(selectedNodeId);
      schemaState = 'ready';
    } catch (e) {
      schemaState = 'error';
      schemaError = `${e}`;
    }
  }

  async function loadPreview() {
    previewState = 'idle';
    previewError = null;
    previewColumns = [];
    previewRows = [];

    const selectedNodeId = pipelineStore.selectedNodeId;
    if (!selectedNodeId) return;
    const node = pipelineStore.nodes.find((n: any) => n.id === selectedNodeId);
    if (!node) return;
    if (node.type !== 'join') return;

    previewState = 'loading';
    try {
      const result = await invoke('preview_pipeline_node', { nodes: pipelineStore.nodes, edges: pipelineStore.edges, nodeId: selectedNodeId });
      const r = result as any;
      previewColumns = Array.isArray(r?.columns) ? r.columns : [];
      previewRows = Array.isArray(r?.rows) ? r.rows : [];
      previewState = 'ready';
    } catch (e) {
      previewState = 'error';
      previewError = `${e}`;
    }
  }

  return {
    get schemaState() { return schemaState; },
    get schemaError() { return schemaError; },
    get schemaFields() { return schemaFields; },
    get previewState() { return previewState; },
    get previewError() { return previewError; },
    get previewColumns() { return previewColumns; },
    get previewRows() { return previewRows; },
    loadSchema,
    loadPreview,
  };
}

export const schemaStore = createSchemaStore();
