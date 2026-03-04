import { invoke } from '@tauri-apps/api/core';
import { save, open } from '@tauri-apps/plugin-dialog';
import { load } from '@tauri-apps/plugin-store';
import { pipelineStore } from './pipelineStore.svelte';

export type PipelineMeta = {
  name: string;
  description: string;
  tags: string[];
};

type RecentEntry = {
  path: string;
  name: string;
  openedAt: number;
};

const STORE_KEY = 'recent_pipelines';
const MAX_RECENT = 10;

function createFileStore() {
  let currentFilePath = $state<string | null>(null);
  let pipelineMeta = $state<PipelineMeta>({ name: 'Untitled Pipeline', description: '', tags: [] });
  let isDirty = $state(false);
  let recentPipelines = $state<RecentEntry[]>([]);
  let showHomeScreen = $state(true);

  function serializePipeline(): string {
    return JSON.stringify({
      version: 1,
      meta: { ...pipelineMeta },
      nodes: pipelineStore.nodes,
      edges: pipelineStore.edges,
    }, null, 2);
  }

  function restorePipeline(json: string) {
    const data = JSON.parse(json);
    if (data.meta) {
      pipelineMeta = {
        name: data.meta.name || 'Untitled Pipeline',
        description: data.meta.description || '',
        tags: Array.isArray(data.meta.tags) ? data.meta.tags : [],
      };
    }
    if (Array.isArray(data.nodes)) {
      pipelineStore.nodes = data.nodes;
    }
    if (Array.isArray(data.edges)) {
      pipelineStore.edges = data.edges;
    }
    isDirty = false;
  }

  async function addToRecent(path: string, name: string) {
    try {
      const store = await load('crossql-store.json', { autoSave: true, defaults: {} });
      const existing = ((await store.get<RecentEntry[]>(STORE_KEY)) ?? []);
      const filtered = existing.filter((e: RecentEntry) => e.path !== path);
      filtered.unshift({ path, name, openedAt: Date.now() });
      const capped = filtered.slice(0, MAX_RECENT);
      await store.set(STORE_KEY, capped);
      recentPipelines = capped;
    } catch (e) {
      console.warn('Failed to update recent pipelines:', e);
    }
  }

  async function loadRecentList() {
    try {
      const store = await load('crossql-store.json', { autoSave: false, defaults: {} });
      recentPipelines = (await store.get<RecentEntry[]>(STORE_KEY)) ?? [];
    } catch {
      recentPipelines = [];
    }
  }

  async function savePipeline() {
    let path = currentFilePath;
    if (!path) {
      const selected = await save({
        title: 'Save Pipeline',
        defaultPath: `${pipelineMeta.name.replace(/[^a-zA-Z0-9_-]/g, '_')}.etl.json`,
        filters: [{ name: 'ETL Pipeline', extensions: ['etl.json'] }],
      });
      if (!selected) return;
      path = selected;
    }
    const data = serializePipeline();
    await invoke('save_pipeline', { path, data });
    currentFilePath = path;
    isDirty = false;
    await addToRecent(path, pipelineMeta.name);
  }

  async function saveAs() {
    const selected = await save({
      title: 'Save Pipeline As',
      defaultPath: `${pipelineMeta.name.replace(/[^a-zA-Z0-9_-]/g, '_')}.etl.json`,
      filters: [{ name: 'ETL Pipeline', extensions: ['etl.json'] }],
    });
    if (!selected) return;
    const data = serializePipeline();
    await invoke('save_pipeline', { path: selected, data });
    currentFilePath = selected;
    isDirty = false;
    await addToRecent(selected, pipelineMeta.name);
  }

  async function loadPipeline(filePath?: string) {
    let path = filePath;
    if (!path) {
      const selected = await open({
        title: 'Open Pipeline',
        multiple: false,
        filters: [{ name: 'ETL Pipeline', extensions: ['etl.json'] }],
      });
      if (!selected) return;
      path = selected;
    }
    const json = await invoke<string>('load_pipeline', { path });
    restorePipeline(json);
    currentFilePath = path;
    showHomeScreen = false;
    await addToRecent(path, pipelineMeta.name);
  }

  function newPipeline() {
    pipelineStore.nodes = [
      {
        id: 'out1',
        type: 'parquet',
        data: { config: { path: '/tmp/output.parquet', compression: 'snappy', row_group_size: 524288 } },
        position: { x: 650, y: 120 },
      }
    ];
    pipelineStore.edges = [];
    pipelineStore.selectedNodeId = null;
    currentFilePath = null;
    pipelineMeta = { name: 'Untitled Pipeline', description: '', tags: [] };
    isDirty = false;
    showHomeScreen = false;
  }

  function markDirty() {
    isDirty = true;
  }

  return {
    get currentFilePath() { return currentFilePath; },
    get pipelineMeta() { return pipelineMeta; },
    set pipelineMeta(v: PipelineMeta) { pipelineMeta = v; isDirty = true; },
    get isDirty() { return isDirty; },
    get recentPipelines() { return recentPipelines; },
    get showHomeScreen() { return showHomeScreen; },
    set showHomeScreen(v: boolean) { showHomeScreen = v; },
    savePipeline,
    saveAs,
    loadPipeline,
    newPipeline,
    loadRecentList,
    markDirty,
  };
}

export const fileStore = createFileStore();
