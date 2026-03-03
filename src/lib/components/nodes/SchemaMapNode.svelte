<script lang="ts">
  import { Handle, Position } from '@xyflow/svelte';

  let { data } = $props();

  type NullMode = 'keep' | 'drop_row' | 'fill_default' | 'error';

  type SchemaMapColumn = {
    source: string;
    rename: string;
    cast: '' | 'Int64' | 'Float64' | 'Boolean' | 'Utf8' | 'Datetime';
    null_mode: NullMode;
    fill_value: string;
  };

  let columns = $state<SchemaMapColumn[]>(
    (data.config?.columns as SchemaMapColumn[] | undefined) ?? []
  );

  function stopFlowEvents(e: Event) {
    e.stopPropagation();
  }

  function updateConfig() {
    data.config = { columns };
  }

  function addColumn() {
    columns = [
      ...columns,
      { source: '', rename: '', cast: '', null_mode: 'keep', fill_value: '' }
    ];
    updateConfig();
  }

  function removeColumn(idx: number) {
    columns = columns.filter((_, i) => i !== idx);
    updateConfig();
  }
</script>

<div class="bg-white border-l-4 border-l-[#C49A3C] border border-warm-border rounded shadow-sm w-[28rem]">
  <div class="px-3 py-2 border-b border-warm-border flex items-center gap-2 bg-warm-bg rounded-t">
    <span class="text-xl">🔀</span>
    <span class="font-bold text-warm-text text-sm">Schema Map</span>
  </div>

  <div class="p-3 flex flex-col gap-3 relative" onpointerdown={stopFlowEvents} onwheel={stopFlowEvents}>
    <Handle type="target" position={Position.Left} class="!bg-[#C49A3C] !w-3 !h-3 !-left-1.5" />
    <Handle type="source" position={Position.Right} class="!bg-[#C49A3C] !w-3 !h-3" />

    <div class="text-xs text-warm-sub font-medium">Columns</div>

    {#if columns.length === 0}
      <div class="text-xs text-warm-muted">Add columns to rename, cast, and handle nulls.</div>
    {:else}
      <div class="border border-warm-border rounded overflow-hidden">
        <div class="grid grid-cols-12 gap-0 bg-warm-panel text-[11px] text-warm-sub font-semibold border-b border-warm-border">
          <div class="col-span-4 px-2 py-1">Column</div>
          <div class="col-span-3 px-2 py-1">Rename</div>
          <div class="col-span-2 px-2 py-1">Type</div>
          <div class="col-span-2 px-2 py-1">Nulls</div>
          <div class="col-span-1 px-2 py-1"></div>
        </div>

        <div class="max-h-56 overflow-auto">
          {#each columns as c, i (i)}
            <div class="grid grid-cols-12 gap-0 border-b border-warm-border last:border-b-0 bg-white">
              <div class="col-span-4 p-1">
                <input
                  type="text"
                  bind:value={c.source}
                  oninput={updateConfig}
                  onpointerdown={stopFlowEvents}
                  placeholder="source_col"
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
                />
              </div>
              <div class="col-span-3 p-1">
                <input
                  type="text"
                  bind:value={c.rename}
                  oninput={updateConfig}
                  onpointerdown={stopFlowEvents}
                  placeholder="optional"
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
                />
              </div>
              <div class="col-span-2 p-1">
                <select
                  bind:value={c.cast}
                  onchange={updateConfig}
                  onpointerdown={stopFlowEvents}
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
                >
                  <option value="">Keep</option>
                  <option value="Int64">Int64</option>
                  <option value="Float64">Float64</option>
                  <option value="Boolean">Boolean</option>
                  <option value="Utf8">Utf8</option>
                  <option value="Datetime">Datetime</option>
                </select>
              </div>
              <div class="col-span-2 p-1">
                <select
                  bind:value={c.null_mode}
                  onchange={updateConfig}
                  onpointerdown={stopFlowEvents}
                  class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none bg-white"
                >
                  <option value="keep">Keep</option>
                  <option value="drop_row">Drop row</option>
                  <option value="fill_default">Fill default</option>
                  <option value="error">Error</option>
                </select>
              </div>
              <div class="col-span-1 p-1 flex items-center justify-end">
                <button
                  type="button"
                  onclick={() => removeColumn(i)}
                  onpointerdown={stopFlowEvents}
                  class="text-xs px-2 py-1 bg-warm-panel border border-warm-border rounded hover:bg-warm-light"
                  aria-label="Remove"
                >
                  ×
                </button>
              </div>

              {#if c.null_mode === 'fill_default'}
                <div class="col-span-12 px-2 pb-2">
                  <div class="grid grid-cols-12 gap-2 items-center">
                    <div class="col-span-3 text-[11px] text-warm-sub">Fill value</div>
                    <div class="col-span-9">
                      <input
                        type="text"
                        bind:value={c.fill_value}
                        oninput={updateConfig}
                        onpointerdown={stopFlowEvents}
                        placeholder="e.g. 0, true, 2024-01-01T00:00:00Z"
                        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
                      />
                    </div>
                  </div>
                </div>
              {/if}
            </div>
          {/each}
        </div>
      </div>
    {/if}

    <button
      type="button"
      onclick={addColumn}
      onpointerdown={stopFlowEvents}
      class="self-start text-xs px-3 py-1.5 bg-warm-panel border border-warm-border rounded hover:bg-warm-light"
    >
      Add column
    </button>
  </div>
</div>
