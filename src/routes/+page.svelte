<script lang="ts">
  import PipelineCanvas from '$lib/components/PipelineCanvas.svelte';
  import { invoke } from "@tauri-apps/api/core";

  // Temporary for verify invoke works
  let invokeResult = $state("Waiting for invoke...");

  async function testInvoke() {
    invokeResult = await invoke("greet", { name: "CrossQL Agent" });
  }
</script>

<div class="flex h-screen w-full bg-warm-bg overflow-hidden">
  <!-- Sidebar Palette -->
  <aside class="w-64 bg-warm-panel border-r border-warm-border p-4 flex flex-col">
    <h2 class="text-warm-text font-bold mb-4">Node Palette</h2>
    <div class="text-warm-muted text-sm italic">
      Draggable nodes will go here (Phase 1)
    </div>
    
    <div class="mt-auto border-t border-warm-border pt-4">
      <button 
        onclick={testInvoke}
        class="w-full px-4 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm"
      >
        Test Backend Invoke
      </button>
      <div class="text-xs text-warm-muted mt-2 truncate">
        {invokeResult}
      </div>
    </div>
  </aside>

  <!-- Main Canvas Area -->
  <main class="flex-1 bg-warm-canvas relative">
    <PipelineCanvas />
  </main>

  <!-- Config Panel -->
  <aside class="w-80 bg-warm-panel border-l border-warm-border p-4">
    <h2 class="text-warm-text font-bold mb-4">Configuration</h2>
    <div class="text-warm-muted text-sm italic">
      Node properties will appear here on selection
    </div>
  </aside>
</div>
