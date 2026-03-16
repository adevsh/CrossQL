import { describe, it, expect, vi, beforeEach } from 'vitest';
import { isSendToEngineVisible, submitToEngine } from './sendToEngineLogic';
import { serializePipelineDefinition } from '$lib/engine/serializePipeline';
import { pipelineStore } from '$lib/stores/pipelineStore.svelte';
import { fileStore } from '$lib/stores/fileStore.svelte';

describe('Send to Engine button', () => {
  let definition: ReturnType<typeof serializePipelineDefinition>;

  beforeEach(() => {
    definition = serializePipelineDefinition(
      pipelineStore.nodes,
      pipelineStore.edges,
      fileStore.pipelineMeta.name
    );
  });

  it('is hidden when engine is disconnected', () => {
    expect(isSendToEngineVisible(false)).toBe(false);
  });

  it('is visible when engine is connected', () => {
    expect(isSendToEngineVisible(true)).toBe(true);
  });

  it('is disabled during submission', async () => {
    const states: boolean[] = [];
    await submitToEngine(
      definition,
      () => new Promise((resolve) => setTimeout(resolve, 10)),
      () => {},
      (value) => states.push(value)
    );
    expect(states[0]).toBe(true);
    expect(states[states.length - 1]).toBe(false);
  });

  it('shows toast on successful submission', async () => {
    const notify = vi.fn();
    await submitToEngine(definition, async () => ({}), notify, () => {});
    expect(notify).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: 'success',
        message: expect.stringContaining('Pipeline sent —')
      })
    );
  });

  it('shows error toast on failed submission', async () => {
    const notify = vi.fn();
    await submitToEngine(
      definition,
      async () => {
        throw new Error('Submit failed: 500');
      },
      notify,
      () => {}
    );
    expect(notify).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: 'error',
        message: expect.stringContaining('Failed to send pipeline:')
      })
    );
  });
});
