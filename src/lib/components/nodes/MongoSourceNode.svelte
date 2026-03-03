<script lang="ts">
  import { Handle, Position } from '@xyflow/svelte';
  
  let { data } = $props();
  
  let uri = $state(data.config?.uri || 'mongodb://localhost:27017');
  let database = $state(data.config?.database || '');
  let collection = $state(data.config?.collection || '');
  let filter = $state(data.config?.filter || '{}');
  let projection = $state(data.config?.projection || '{}');
  let flattenDepth = $state(data.config?.flatten_depth ?? 1);

  function updateConfig() {
    data.config = { uri, database, collection, filter, projection, flatten_depth: flattenDepth };
  }
</script>

<div class="bg-white border-l-4 border-l-[#4A7C59] border border-warm-border rounded shadow-sm w-80">
  <div class="px-3 py-2 border-b border-warm-border flex items-center gap-2 bg-warm-bg rounded-t">
    <span class="text-xl">🍃</span>
    <span class="font-bold text-warm-text text-sm">MongoDB Source</span>
  </div>

  <div class="p-3 flex flex-col gap-3 relative">
    <div>
      <label class="text-xs text-warm-sub font-medium">URI</label>
      <input
        type="text"
        bind:value={uri}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
      />
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
        <label class="text-xs text-warm-sub font-medium">Collection</label>
        <input
          type="text"
          bind:value={collection}
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
    </div>

    <div class="grid grid-cols-2 gap-2">
      <div>
        <label class="text-xs text-warm-sub font-medium">Flatten Depth</label>
        <input
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
      <label class="text-xs text-warm-sub font-medium">Filter (JSON)</label>
      <textarea
        bind:value={filter}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none h-20 font-mono"
      ></textarea>
    </div>

    <div>
      <label class="text-xs text-warm-sub font-medium">Projection (JSON)</label>
      <textarea
        bind:value={projection}
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none h-16 font-mono"
      ></textarea>
    </div>

    <Handle type="source" position={Position.Right} class="!bg-[#4A7C59] !w-3 !h-3 !-right-1.5" />
  </div>
</div>

