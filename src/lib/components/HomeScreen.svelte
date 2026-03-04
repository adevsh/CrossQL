<script lang="ts">
  import { fileStore } from '$lib/stores/fileStore.svelte';
  import { onMount } from 'svelte';

  onMount(() => {
    void fileStore.loadRecentList();
  });

  function formatDate(ts: number) {
    return new Date(ts).toLocaleDateString(undefined, {
      month: 'short', day: 'numeric', year: 'numeric',
      hour: '2-digit', minute: '2-digit'
    });
  }
</script>

<div class="absolute inset-0 z-50 flex items-center justify-center bg-warm-bg/95 backdrop-blur-sm">
  <div class="w-full max-w-lg bg-white rounded-xl shadow-lg border border-warm-border p-8 space-y-6">
    <!-- Header -->
    <div class="text-center space-y-2">
      <h1 class="text-3xl font-bold text-warm-text tracking-tight">CrossQL</h1>
      <p class="text-warm-sub text-sm">Visual ETL Pipeline Builder</p>
    </div>

    <!-- Actions -->
    <div class="flex gap-3">
      <button
        onclick={() => fileStore.newPipeline()}
        class="flex-1 px-4 py-3 bg-[#C07A3A] text-white rounded-lg font-medium hover:bg-[#A8672E] transition-colors text-sm"
      >
        ✨ New Pipeline
      </button>
      <button
        onclick={() => void fileStore.loadPipeline()}
        class="flex-1 px-4 py-3 bg-white border border-warm-border rounded-lg text-warm-text font-medium hover:bg-warm-light transition-colors text-sm"
      >
        📂 Open Pipeline
      </button>
    </div>

    <!-- Recent Pipelines -->
    {#if fileStore.recentPipelines.length > 0}
      <div class="space-y-2">
        <h3 class="text-xs font-semibold text-warm-sub uppercase tracking-wider">Recent Pipelines</h3>
        <div class="space-y-1 max-h-48 overflow-y-auto">
          {#each fileStore.recentPipelines as entry}
            <button
              onclick={() => void fileStore.loadPipeline(entry.path)}
              class="w-full flex items-center justify-between px-3 py-2 rounded-lg hover:bg-warm-panel transition-colors text-left group"
            >
              <div class="min-w-0">
                <div class="text-sm font-medium text-warm-text truncate">{entry.name}</div>
                <div class="text-xs text-warm-muted truncate">{entry.path}</div>
              </div>
              <span class="text-xs text-warm-muted whitespace-nowrap ml-3">
                {formatDate(entry.openedAt)}
              </span>
            </button>
          {/each}
        </div>
      </div>
    {/if}

    <!-- Skip -->
    <div class="text-center">
      <button
        onclick={() => fileStore.showHomeScreen = false}
        class="text-xs text-warm-muted hover:text-warm-sub transition-colors"
      >
        Continue with current canvas →
      </button>
    </div>
  </div>
</div>
