<script lang="ts">
  import { Handle, Position } from '@xyflow/svelte';
  
  let { data } = $props();
  
  let contactPoints = $state(data.config?.contact_points || 'localhost:9042');
  let keyspace = $state(data.config?.keyspace || '');
  let query = $state(data.config?.query || 'SELECT * FROM table');

  function updateConfig() {
    data.config = { contact_points: contactPoints, keyspace, query };
  }
</script>

<div class="bg-white border-l-4 border-l-[#6B5A9B] border border-warm-border rounded shadow-sm w-80">
  <div class="px-3 py-2 border-b border-warm-border flex items-center gap-2 bg-warm-bg rounded-t">
    <span class="text-xl">👁</span>
    <span class="font-bold text-warm-text text-sm">Cassandra Source</span>
  </div>

  <div class="p-3 flex flex-col gap-3 relative">
    <div>
      <label class="text-xs text-warm-sub font-medium">Contact Points</label>
      <input
        type="text"
        bind:value={contactPoints}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
      />
      <div class="text-[11px] text-warm-muted mt-1">Comma-separated, e.g. localhost:9042,10.0.0.2:9042</div>
    </div>

    <div>
      <label class="text-xs text-warm-sub font-medium">Keyspace</label>
      <input
        type="text"
        bind:value={keyspace}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
      />
    </div>

    <div>
      <label class="text-xs text-warm-sub font-medium">CQL Query</label>
      <textarea
        bind:value={query}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none h-20 font-mono"
      ></textarea>
    </div>

    <Handle type="source" position={Position.Right} class="!bg-[#6B5A9B] !w-3 !h-3 !-right-1.5" />
  </div>
</div>

