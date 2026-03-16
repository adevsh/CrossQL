<script lang="ts">
  import PipelineCanvas from '$lib/components/PipelineCanvas.svelte';
  import SidebarPalette from '$lib/components/SidebarPalette.svelte';
  import ConfigPanel from '$lib/components/ConfigPanel.svelte';
  import HomeScreen from '$lib/components/HomeScreen.svelte';
  import { pipelineStore } from '$lib/stores/pipelineStore.svelte';
  import { executionStore } from '$lib/stores/executionStore.svelte';
  import { schemaStore } from '$lib/stores/schemaStore.svelte';
  import { fileStore } from '$lib/stores/fileStore.svelte';
  import { onMount } from 'svelte';

  onMount(() => {
    const unlistenPromise = executionStore.setupEventListener();

    void executionStore.loadUsage();
    const usageInterval = setInterval(() => void executionStore.loadUsage(), 5000);

    // Keyboard shortcuts
    function handleKeydown(e: KeyboardEvent) {
      if (e.metaKey || e.ctrlKey) {
        if (e.key === 's') {
          e.preventDefault();
          void fileStore.savePipeline();
        } else if (e.key === 'o') {
          e.preventDefault();
          void fileStore.loadPipeline();
        } else if (e.key === 'n') {
          e.preventDefault();
          fileStore.newPipeline();
        }
      }
    }
    window.addEventListener('keydown', handleKeydown);

    return () => {
      unlistenPromise.then((u) => u());
      clearInterval(usageInterval);
      clearTimeout(schemaTimer);
      clearTimeout(previewTimer);
      window.removeEventListener('keydown', handleKeydown);
    };
  });

  let schemaTimer: ReturnType<typeof setTimeout>;
  let previewTimer: ReturnType<typeof setTimeout>;

  $effect(() => {
    const _sel = pipelineStore.selectedNodeId;
    const _nodes = pipelineStore.nodes;
    clearTimeout(schemaTimer);
    schemaTimer = setTimeout(() => void schemaStore.loadSchema(), 300);
  });

  $effect(() => {
    const _sel = pipelineStore.selectedNodeId;
    const _nodes = pipelineStore.nodes;
    clearTimeout(previewTimer);
    previewTimer = setTimeout(() => void schemaStore.loadPreview(), 300);
  });
</script>

<div class="flex h-[calc(100vh-49px)] w-full bg-warm-bg overflow-hidden">
  <SidebarPalette />

  <!-- Main Canvas Area -->
  <main class="flex-1 bg-warm-canvas relative">
    <PipelineCanvas bind:nodes={pipelineStore.nodes} bind:edges={pipelineStore.edges} bind:selectedNodeId={pipelineStore.selectedNodeId} />
    {#if fileStore.showHomeScreen}
      <HomeScreen />
    {/if}
  </main>

  <ConfigPanel />
</div>
