<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';
  
  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();
  
  let uri = $state('mongodb://localhost:27017');
  let database = $state('');
  let collection = $state('');
  let filter = $state('{}');
  let projection = $state('{}');
  let flattenDepth = $state(1);

  function statusRingClass() {
    const s = data?.run_state;
    if (s === 'running') return 'border-accent animate-pulse bg-accent-light';
    if (s === 'done') return 'border-[#4A7C59] bg-[#4A7C59]';
    if (s === 'error') return 'border-[#B85C4A] bg-[#B85C4A]';
    return 'border-warm-border bg-white';
  }

  function updateConfig() {
    data.config = { uri, database, collection, filter, projection, flatten_depth: flattenDepth };
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
    const nextUri = typeof cfg.uri === 'string' ? cfg.uri : 'mongodb://localhost:27017';
    const nextDatabase = typeof cfg.database === 'string' ? cfg.database : '';
    const nextCollection = typeof cfg.collection === 'string' ? cfg.collection : '';
    const nextFilter = typeof cfg.filter === 'string' ? cfg.filter : '{}';
    const nextProjection = typeof cfg.projection === 'string' ? cfg.projection : '{}';
    const nextFlattenDepth = typeof cfg.flatten_depth === 'number' ? cfg.flatten_depth : 1;
    if (uri !== nextUri) uri = nextUri;
    if (database !== nextDatabase) database = nextDatabase;
    if (collection !== nextCollection) collection = nextCollection;
    if (filter !== nextFilter) filter = nextFilter;
    if (projection !== nextProjection) projection = nextProjection;
    if (flattenDepth !== nextFlattenDepth) flattenDepth = nextFlattenDepth;
  });
</script>

<div class="bg-white border-l-4 border-l-[#4A7C59] border border-warm-border rounded shadow-sm w-80">
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">🍃</span>
      <span class="font-bold text-warm-text text-sm">MongoDB Source</span>
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
      <label for="mongo-uri-{id}" class="text-xs text-warm-sub font-medium">URI</label>
      <input
        id="mongo-uri-{id}"
        type="text"
        bind:value={uri}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
      />
    </div>

    <div class="grid grid-cols-2 gap-2">
      <div>
        <label for="mongo-database-{id}" class="text-xs text-warm-sub font-medium">Database</label>
        <input
          id="mongo-database-{id}"
          type="text"
          bind:value={database}
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
      <div>
        <label for="mongo-collection-{id}" class="text-xs text-warm-sub font-medium">Collection</label>
        <input
          id="mongo-collection-{id}"
          type="text"
          bind:value={collection}
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
    </div>

    <div class="grid grid-cols-2 gap-2">
      <div>
        <label for="mongo-flatten-depth-{id}" class="text-xs text-warm-sub font-medium">Flatten Depth</label>
        <input
          id="mongo-flatten-depth-{id}"
          type="number"
          min="0"
          max="5"
          bind:value={flattenDepth}
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
      <div class="text-xs text-warm-muted self-end">
        0 = none
      </div>
    </div>

    <div>
      <label for="mongo-filter-{id}" class="text-xs text-warm-sub font-medium">Filter (JSON)</label>
      <textarea
        id="mongo-filter-{id}"
        bind:value={filter}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none h-20 font-mono"
      ></textarea>
    </div>

    <div>
      <label for="mongo-projection-{id}" class="text-xs text-warm-sub font-medium">Projection (JSON)</label>
      <textarea
        id="mongo-projection-{id}"
        bind:value={projection}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none h-16 font-mono"
      ></textarea>
    </div>
    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}

    <Handle type="source" position={Position.Right} class="!bg-[#4A7C59] !w-3 !h-3 !-right-1.5" />
  </div>
</div>
