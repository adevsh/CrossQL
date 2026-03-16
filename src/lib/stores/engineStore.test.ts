import { describe, it, expect, vi, beforeEach } from 'vitest';
import { get } from 'svelte/store';
import { createEngineStore } from './engineStore';

function createMemoryStorage(): Storage {
  const map = new Map<string, string>();
  return {
    get length() {
      return map.size;
    },
    clear() {
      map.clear();
    },
    getItem(key: string) {
      return map.has(key) ? map.get(key)! : null;
    },
    key(index: number) {
      return Array.from(map.keys())[index] ?? null;
    },
    removeItem(key: string) {
      map.delete(key);
    },
    setItem(key: string, value: string) {
      map.set(key, value);
    }
  };
}

describe('engineStore', () => {
  let storage: Storage;

  beforeEach(() => {
    vi.restoreAllMocks();
    storage = createMemoryStorage();
  });

  it('defaults to disconnected state on initialisation', () => {
    const store = createEngineStore(storage, vi.fn() as unknown as typeof fetch);
    const state = get(store.state);
    expect(state.connected).toBe(false);
    expect(state.uptime).toBeNull();
  });

  it('sets connected to true when health check returns 200', async () => {
    const fetcher = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ uptime_secs: 12 })
    });
    const store = createEngineStore(storage, fetcher as unknown as typeof fetch);
    const ok = await store.connect();
    const state = get(store.state);
    expect(ok).toBe(true);
    expect(state.connected).toBe(true);
    expect(state.uptime).toBe(12);
  });

  it('sets connected to false when health check returns non-200', async () => {
    const fetcher = vi.fn().mockResolvedValue({ ok: false, status: 503 });
    const store = createEngineStore(storage, fetcher as unknown as typeof fetch);
    const ok = await store.connect();
    const state = get(store.state);
    expect(ok).toBe(false);
    expect(state.connected).toBe(false);
  });

  it('sets connected to false when health check throws network error', async () => {
    const fetcher = vi.fn().mockRejectedValue(new Error('network'));
    const store = createEngineStore(storage, fetcher as unknown as typeof fetch);
    const ok = await store.connect();
    const state = get(store.state);
    expect(ok).toBe(false);
    expect(state.connected).toBe(false);
  });

  it('persists url and apiKey after store update', () => {
    const store = createEngineStore(storage, vi.fn() as unknown as typeof fetch);
    store.setUrl('http://192.168.1.100:7070');
    store.setApiKey('key-123');
    const reloaded = createEngineStore(storage, vi.fn() as unknown as typeof fetch);
    const state = get(reloaded.state);
    expect(state.url).toBe('http://192.168.1.100:7070');
    expect(state.apiKey).toBe('key-123');
  });
});
