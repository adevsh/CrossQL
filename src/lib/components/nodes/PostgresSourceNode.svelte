<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';
  
  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();
  function stopFlowEvents(e: Event) {
    e.stopPropagation();
  }
  
  // Local state for form inputs
  let host = $state(data.config?.host || 'localhost');
  let port = $state(data.config?.port || 5432);
  let database = $state(data.config?.database || '');
  let user = $state(data.config?.user || '');
  let password = $state(data.config?.password || '');
  let query = $state(data.config?.query || 'SELECT * FROM table');

  function statusRingClass() {
    const s = data?.run_state;
    if (s === 'running') return 'border-accent animate-pulse bg-accent-light';
    if (s === 'done') return 'border-[#4A7C59] bg-[#4A7C59]';
    if (s === 'error') return 'border-[#B85C4A] bg-[#B85C4A]';
    return 'border-warm-border bg-white';
  }

  function updateConfig() {
    data.config = { host, port, database, user, password, query };
  }

  async function removeNode(e: Event) {
    e.stopPropagation();
    await deleteElements({ nodes: [{ id }] });
  }
</script>

<div class="bg-white border-l-4 border-l-[#4A7A9B] border border-warm-border rounded shadow-sm w-80">
  <!-- Header -->
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">🐘</span>
      <span class="font-bold text-warm-text text-sm">PostgreSQL Source</span>
    </div>
    <button
      type="button"
      onclick={removeNode}
      onpointerdown={stopFlowEvents}
      class="nodrag w-6 h-6 flex items-center justify-center rounded hover:bg-warm-light text-warm-sub"
      aria-label="Remove node"
      title="Remove"
    >
      ×
    </button>
  </div>

  <!-- Body -->
  <div class="nodrag p-3 flex flex-col gap-3">
    <div class="grid grid-cols-3 gap-2">
      <div class="col-span-2">
        <label class="text-xs text-warm-sub font-medium">Host</label>
        <input 
          type="text" 
          bind:value={host} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
      <div>
        <label class="text-xs text-warm-sub font-medium">Port</label>
        <input 
          type="number" 
          bind:value={port} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
    </div>

    <div class="grid grid-cols-2 gap-2">
      <div>
        <label class="text-xs text-warm-sub font-medium">Database</label>
        <input 
          type="text" 
          bind:value={database} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
      <div>
        <label class="text-xs text-warm-sub font-medium">User</label>
        <input 
          type="text" 
          bind:value={user} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
    </div>

    <div>
      <label class="text-xs text-warm-sub font-medium">Password</label>
      <input 
        type="password" 
        bind:value={password} 
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
      />
    </div>

    <div>
      <label class="text-xs text-warm-sub font-medium">SQL Query</label>
      <textarea 
        bind:value={query} 
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none h-20 font-mono"
      ></textarea>
    </div>
    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}
  </div>

  <!-- Output Handle -->
  <Handle type="source" position={Position.Right} class="!bg-[#4A7A9B] !w-3 !h-3" />
</div>
