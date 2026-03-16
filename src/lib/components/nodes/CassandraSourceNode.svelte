<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';
  
  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();
  
  let contactPoints = $state('localhost:9042');
  let keyspace = $state('');
  let query = $state('SELECT * FROM table');

  function statusRingClass() {
    const s = data?.run_state;
    if (s === 'running') return 'border-accent animate-pulse bg-accent-light';
    if (s === 'done') return 'border-[#4A7C59] bg-[#4A7C59]';
    if (s === 'error') return 'border-[#B85C4A] bg-[#B85C4A]';
    return 'border-warm-border bg-white';
  }

  function updateConfig() {
    data.config = { contact_points: contactPoints, keyspace, query };
  }

  function stopFlowEvents(e: Event) {
    e.stopPropagation();
  }

  async function removeNode(e: Event) {
    e.stopPropagation();
    await deleteElements({ nodes: [{ id }] });
  }

  $effect(() => {
    const cfg = data?.config ?? {};
    const nextContactPoints = typeof cfg.contact_points === 'string' ? cfg.contact_points : 'localhost:9042';
    const nextKeyspace = typeof cfg.keyspace === 'string' ? cfg.keyspace : '';
    const nextQuery = typeof cfg.query === 'string' ? cfg.query : 'SELECT * FROM table';
    if (contactPoints !== nextContactPoints) contactPoints = nextContactPoints;
    if (keyspace !== nextKeyspace) keyspace = nextKeyspace;
    if (query !== nextQuery) query = nextQuery;
  });
</script>

<div class="bg-white border-l-4 border-l-[#6B5A9B] border border-warm-border rounded shadow-sm w-80">
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">👁</span>
      <span class="font-bold text-warm-text text-sm">Cassandra Source</span>
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

  <div class="nodrag p-3 flex flex-col gap-3 relative">
    <div>
      <label for="cassandra-contact-points-{id}" class="text-xs text-warm-sub font-medium">Contact Points</label>
      <input
        id="cassandra-contact-points-{id}"
        type="text"
        bind:value={contactPoints}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
      />
      <div class="text-[11px] text-warm-muted mt-1">Comma-separated, e.g. localhost:9042,10.0.0.2:9042</div>
    </div>

    <div>
      <label for="cassandra-keyspace-{id}" class="text-xs text-warm-sub font-medium">Keyspace</label>
      <input
        id="cassandra-keyspace-{id}"
        type="text"
        bind:value={keyspace}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
      />
    </div>

    <div>
      <label for="cassandra-query-{id}" class="text-xs text-warm-sub font-medium">CQL Query</label>
      <textarea
        id="cassandra-query-{id}"
        bind:value={query}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none h-20 font-mono"
      ></textarea>
    </div>
    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}

    <Handle type="source" position={Position.Right} class="!bg-[#6B5A9B] !w-3 !h-3 !-right-1.5" />
  </div>
</div>
