<script lang="ts">
  import { Handle, Position } from '@xyflow/svelte';
  import { save } from '@tauri-apps/plugin-dialog';

  let { data } = $props();

  let path = $state(data.config?.path || '');
  let compression = $state(data.config?.compression || 'snappy');
  let rowGroupSize = $state(data.config?.row_group_size || 524288);

  function updateConfig() {
    data.config = { path, compression, row_group_size: rowGroupSize };
  }

  async function browseFile() {
    const selected = await save({
      filters: [{
        name: 'Parquet',
        extensions: ['parquet']
      }]
    });
    if (selected) {
      path = selected;
      updateConfig();
    }
  }
</script>

<div class="bg-white border-l-4 border-l-[#4A7C59] border border-warm-border rounded shadow-sm w-72">
  <!-- Header -->
  <div class="px-3 py-2 border-b border-warm-border flex items-center gap-2 bg-warm-bg rounded-t">
    <span class="text-xl">📦</span>
    <span class="font-bold text-warm-text text-sm">Parquet Output</span>
  </div>

  <!-- Body -->
  <div class="p-3 flex flex-col gap-3 relative">
    <!-- Input Handle -->
    <Handle type="target" position={Position.Left} class="!bg-[#4A7C59] !w-3 !h-3 !-left-1.5" />

    <div>
      <label class="text-xs text-warm-sub font-medium">Output Path</label>
      <div class="flex gap-1">
        <input 
          type="text" 
          bind:value={path} 
          oninput={updateConfig}
          class="flex-1 text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
        <button 
          onclick={browseFile}
          class="text-xs px-2 py-1 bg-warm-panel border border-warm-border rounded hover:bg-warm-light"
        >
          ...
        </button>
      </div>
    </div>

    <div class="grid grid-cols-2 gap-2">
      <div>
        <label class="text-xs text-warm-sub font-medium">Compression</label>
        <select 
          bind:value={compression} 
          onchange={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
        >
          <option value="snappy">Snappy</option>
          <option value="zstd">Zstd</option>
          <option value="gzip">Gzip</option>
          <option value="lz4">Lz4</option>
          <option value="none">None</option>
        </select>
      </div>
      <div>
        <label class="text-xs text-warm-sub font-medium">Row Group</label>
        <input 
          type="number" 
          bind:value={rowGroupSize} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
    </div>
  </div>
</div>
