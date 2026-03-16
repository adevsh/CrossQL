<script lang="ts">
  import { pipelineStore } from '$lib/stores/pipelineStore.svelte';
  import { fileStore } from '$lib/stores/fileStore.svelte';
  import SendToEngineButton from '$lib/components/SendToEngineButton.svelte';
  import PipelineMetaForm from './PipelineMetaForm.svelte';

  function addNode(type: Parameters<typeof pipelineStore.addNode>[0]) {
    pipelineStore.addNode(type);
    fileStore.markDirty();
  }
</script>

<aside class="w-64 bg-warm-panel border-r border-warm-border flex flex-col overflow-y-auto">
  <!-- Pipeline Meta -->
  <PipelineMetaForm />

  <!-- File Actions -->
  <div class="px-4 py-2 border-b border-warm-border flex gap-1">
    <button
      onclick={() => fileStore.newPipeline()}
      class="flex-1 px-2 py-1.5 text-xs bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors"
      title="New Pipeline (⌘N)"
    >New</button>
    <button
      onclick={() => void fileStore.loadPipeline()}
      class="flex-1 px-2 py-1.5 text-xs bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors"
      title="Open Pipeline (⌘O)"
    >Open</button>
    <button
      onclick={() => void fileStore.savePipeline()}
      class="flex-1 px-2 py-1.5 text-xs bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors"
      title="Save Pipeline (⌘S)"
    >Save</button>
    <SendToEngineButton />
  </div>

  <!-- Node Palette -->
  <div class="p-4">
  <h2 class="text-warm-text font-bold mb-4">Node Palette</h2>
  <div class="flex flex-col gap-4">
    <div>
      <div class="text-xs text-warm-sub font-semibold mb-2">Sources</div>
      <div class="flex flex-col gap-2">
        <button
          type="button"
          onclick={() => addNode('postgres')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🐘</span><span>PostgreSQL</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('mysql')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🐬</span><span>MySQL</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('mongodb')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🍃</span><span>MongoDB</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('cassandra')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>👁</span><span>Cassandra</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('csv_source')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🧾</span><span>CSV Source</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('parquet_source')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🪵</span><span>Parquet Source</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('file')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>📂</span><span>File Source (Any)</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
      </div>
    </div>

    <div>
      <div class="text-xs text-warm-sub font-semibold mb-2">Outputs</div>
      <div class="flex flex-col gap-2">
        <button
          type="button"
          onclick={() => addNode('parquet')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>📦</span><span>Parquet</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
      </div>
    </div>

    <div>
      <div class="text-xs text-warm-sub font-semibold mb-2">Transforms</div>
      <div class="flex flex-col gap-2">
        <button
          type="button"
          onclick={() => addNode('schema_map')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🔀</span><span>Schema Map</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('join')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🎲</span><span>Join</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('filter')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🔽</span><span>Filter</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('select')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>✅</span><span>Select</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('rename')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>✏️</span><span>Rename</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('cast')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>🧬</span><span>Cast</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
        <button
          type="button"
          onclick={() => addNode('derived')}
          class="w-full px-3 py-2 bg-white border border-warm-border rounded text-warm-text hover:bg-warm-light transition-colors text-sm flex items-center justify-between"
        >
          <span class="flex items-center gap-2"><span>➕</span><span>Derived</span></span>
          <span class="text-warm-muted text-xs">Add</span>
        </button>
      </div>
    </div>
  </div>
  </div>
</aside>
