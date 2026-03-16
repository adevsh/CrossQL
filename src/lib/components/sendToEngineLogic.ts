import type { PipelineDefinition } from '$lib/engine/serializePipeline';

export function isSendToEngineVisible(connected: boolean) {
  return connected;
}

export async function submitToEngine(
  definition: PipelineDefinition,
  submit: (definition: PipelineDefinition) => Promise<unknown>,
  notify: (payload: { kind: 'success' | 'error'; message: string; linkText?: string; href?: string }) => void,
  setSubmitting: (value: boolean) => void
) {
  setSubmitting(true);
  try {
    await submit(definition);
    notify({
      kind: 'success',
      message: `Pipeline sent — ${definition.pipeline_id} v${definition.pipeline_version} queued`,
      linkText: 'View in Remote →',
      href: '/remote'
    });
  } catch (error) {
    notify({
      kind: 'error',
      message: `Failed to send pipeline: ${error}`
    });
  } finally {
    setSubmitting(false);
  }
}
