<script lang="ts">
  import PipelineCanvas from '$lib/components/PipelineCanvas.svelte';
  import SidebarPalette from '$lib/components/SidebarPalette.svelte';
  import ConfigPanel from '$lib/components/ConfigPanel.svelte';
  import { pipelineStore } from '$lib/stores/pipelineStore.svelte';
  import { executionStore } from '$lib/stores/executionStore.svelte';
  import { schemaStore } from '$lib/stores/schemaStore.svelte';
  import { onMount } from 'svelte';

  onMount(() => {
    const unlistenPromise = executionStore.setupEventListener();

    void executionStore.loadUsage();
    const usageInterval = setInterval(() => void executionStore.loadUsage(), 5000);

    return () => {
      unlistenPromise.then((u) => u());
      clearInterval(usageInterval);
    };
  });

  $effect(() => {
    void schemaStore.loadSchema();
  });

  $effect(() => {
    void schemaStore.loadPreview();
  });
</script>

<div class="flex h-screen w-full bg-warm-bg overflow-hidden">
  <SidebarPalette />

  <!-- Main Canvas Area -->
  <main class="flex-1 bg-warm-canvas relative">
    <PipelineCanvas bind:nodes={pipelineStore.nodes} bind:edges={pipelineStore.edges} bind:selectedNodeId={pipelineStore.selectedNodeId} />
  </main>

  <ConfigPanel />
</div>
