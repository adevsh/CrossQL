import { browser } from '$app/environment';
import { get, writable, type Writable } from 'svelte/store';
import type { PipelineDefinition } from '$lib/engine/serializePipeline';

export type EngineConfig = {
  url: string;
  apiKey: string;
  connected: boolean;
  uptime: number | null;
};

type PersistedEngineConfig = {
  url: string;
  apiKey: string;
};

const STORAGE_KEY = 'crossql_engine_config_v1';

const defaultConfig: EngineConfig = {
  url: 'http://127.0.0.1:7070',
  apiKey: '',
  connected: false,
  uptime: null
};

function normalizeUrl(url: string): string {
  return url.trim().replace(/\/+$/, '');
}

function loadPersisted(storage: Storage | null): PersistedEngineConfig | null {
  if (!storage) return null;
  try {
    const raw = storage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as PersistedEngineConfig;
    if (typeof parsed.url !== 'string' || typeof parsed.apiKey !== 'string') return null;
    return parsed;
  } catch {
    return null;
  }
}

function savePersisted(storage: Storage | null, cfg: EngineConfig) {
  if (!storage) return;
  const payload: PersistedEngineConfig = { url: cfg.url, apiKey: cfg.apiKey };
  storage.setItem(STORAGE_KEY, JSON.stringify(payload));
}

export function createEngineStore(
  storage: Storage | null,
  fetcher?: typeof fetch
): {
  state: Writable<EngineConfig>;
  getSnapshot: () => EngineConfig;
  setUrl: (url: string) => void;
  setApiKey: (apiKey: string) => void;
  setConnection: (connected: boolean, uptime: number | null) => void;
  setDisconnected: () => void;
  connect: () => Promise<boolean>;
  fetchStatus: () => Promise<{ active_runs: number; queued: number; completed_total: number }>;
  fetchRuns: () => Promise<any[]>;
  fetchPipelineStatus: (jobId: string) => Promise<any>;
  submitPipeline: (definition: PipelineDefinition) => Promise<{ job_id: string; queued_at: string }>;
  cancelPipeline: (jobId: string) => Promise<boolean>;
} {
  const persisted = loadPersisted(storage);
  const initial = {
    ...defaultConfig,
    ...(persisted ? { url: normalizeUrl(persisted.url), apiKey: persisted.apiKey } : {})
  };

  const state = writable<EngineConfig>(initial);
  state.subscribe((cfg) => savePersisted(storage, cfg));

  function getSnapshot() {
    return get(state);
  }

  function setUrl(url: string) {
    state.update((cfg) => ({ ...cfg, url: normalizeUrl(url) }));
  }

  function setApiKey(apiKey: string) {
    state.update((cfg) => ({ ...cfg, apiKey }));
  }

  function setConnection(connected: boolean, uptime: number | null) {
    state.update((cfg) => ({ ...cfg, connected, uptime }));
  }

  function setDisconnected() {
    state.update((cfg) => ({ ...cfg, connected: false, uptime: null }));
  }

  async function request(path: string, init?: RequestInit) {
    const cfg = getSnapshot();
    const runFetch = fetcher ?? globalThis.fetch.bind(globalThis);
    const headers = new Headers(init?.headers ?? {});
    if (cfg.apiKey) headers.set('Authorization', `Bearer ${cfg.apiKey}`);
    if (!headers.has('Content-Type') && init?.body) headers.set('Content-Type', 'application/json');
    const response = await runFetch(`${normalizeUrl(cfg.url)}${path}`, {
      ...init,
      headers
    });
    return response;
  }

  async function connect() {
    try {
      const response = await request('/health', { method: 'GET' });
      if (!response.ok) {
        setDisconnected();
        return false;
      }
      const body = (await response.json()) as { uptime_secs?: number };
      state.update((cfg) => ({
        ...cfg,
        connected: true,
        uptime: typeof body?.uptime_secs === 'number' ? body.uptime_secs : null
      }));
      return true;
    } catch {
      setDisconnected();
      return false;
    }
  }

  async function fetchStatus() {
    const response = await request('/status', { method: 'GET' });
    if (!response.ok) throw new Error(`Status request failed: ${response.status}`);
    return (await response.json()) as { active_runs: number; queued: number; completed_total: number };
  }

  async function fetchRuns() {
    const response = await request('/runs', { method: 'GET' });
    if (!response.ok) throw new Error(`Runs request failed: ${response.status}`);
    return (await response.json()) as any[];
  }

  async function fetchPipelineStatus(jobId: string) {
    const response = await request(`/pipeline/${jobId}/status`, { method: 'GET' });
    if (!response.ok) throw new Error(`Pipeline status request failed: ${response.status}`);
    return await response.json();
  }

  async function submitPipeline(definition: PipelineDefinition) {
    const response = await request('/pipeline/submit', {
      method: 'POST',
      body: JSON.stringify(definition)
    });
    if (!response.ok) throw new Error(`Submit failed: ${response.status}`);
    return (await response.json()) as { job_id: string; queued_at: string };
  }

  async function cancelPipeline(jobId: string) {
    const response = await request(`/pipeline/${jobId}`, { method: 'DELETE' });
    if (!response.ok) return false;
    const body = (await response.json()) as { cancelled?: boolean };
    return !!body.cancelled;
  }

  return {
    state,
    getSnapshot,
    setUrl,
    setApiKey,
    setConnection,
    setDisconnected,
    connect,
    fetchStatus,
    fetchRuns,
    fetchPipelineStatus,
    submitPipeline,
    cancelPipeline
  };
}

const storage = browser ? window.localStorage : null;
export const engineStore = createEngineStore(storage);
export const engineState = engineStore.state;
