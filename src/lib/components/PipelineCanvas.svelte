<script lang="ts">
  import { SvelteFlow, Background, Controls, MiniMap } from '@xyflow/svelte';
  import '@xyflow/svelte/dist/style.css';

  // Temporary initial state for Phase 1 verification
  let nodes = $state([
    {
      id: '1',
      type: 'input',
      data: { label: 'Start Phase 1' },
      position: { x: 250, y: 25 },
      style: "background: #FFFFFF; border: 1px solid #DDD5C8; color: #2C2416; width: 180px;"
    },
  ]);
  let edges = $state([]);
</script>

<div class="h-full w-full bg-warm-canvas">
  <SvelteFlow 
    bind:nodes 
    bind:edges 
    fitView
    class="bg-warm-canvas"
  >
    <Background patternColor="#D9D0C0" gap={20} size={1} />
    <Controls showLock={false} /> 
    <MiniMap nodeColor="#C07A3A" maskColor="#F5F0E8" />
  </SvelteFlow>
</div>
