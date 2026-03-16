<script lang="ts">
  import { engineState, engineStore } from '$lib/stores/engineStore';
  import { pipelineStore } from '$lib/stores/pipelineStore.svelte';
  import { fileStore } from '$lib/stores/fileStore.svelte';
  import { toastStore } from '$lib/stores/toastStore';
  import { serializePipelineDefinition } from '$lib/engine/serializePipeline';
  import { isSendToEngineVisible, submitToEngine } from '$lib/components/sendToEngineLogic';

  let submitting = $state(false);

  async function sendToEngine() {
    if (submitting) return;
    const definition = serializePipelineDefinition(
      pipelineStore.nodes,
      pipelineStore.edges,
      fileStore.pipelineMeta.name
    );
    await submitToEngine(
      definition,
      (payload) => engineStore.submitPipeline(payload),
      (payload) => toastStore.push(payload),
      (value) => {
        submitting = value;
      }
    );
  }
</script>

{#if isSendToEngineVisible($engineState.connected)}
  <button
    type="button"
    onclick={() => void sendToEngine()}
    class="flex-1 px-2 py-1.5 text-xs bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors"
    disabled={submitting}
  >
    {submitting ? 'Sending…' : 'Send to Engine'}
  </button>
{/if}
