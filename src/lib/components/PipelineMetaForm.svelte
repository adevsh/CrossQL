<script lang="ts">
  import { tick } from 'svelte';
  import { fileStore } from '$lib/stores/fileStore.svelte';

  let editingName = $state(false);
  let nameInput = $state(fileStore.pipelineMeta.name);
  let nameInputEl = $state<HTMLInputElement | null>(null);

  function commitName() {
    if (nameInput.trim()) {
      fileStore.pipelineMeta = { ...fileStore.pipelineMeta, name: nameInput.trim() };
    }
    editingName = false;
  }

  $effect(() => {
    if (!editingName) return;
    tick().then(() => {
      nameInputEl?.focus();
      nameInputEl?.select();
    });
  });
</script>

<div class="px-4 py-3 border-b border-warm-border space-y-1">
  {#if editingName}
    <input
      bind:this={nameInputEl}
      type="text"
      bind:value={nameInput}
      onblur={commitName}
      onkeydown={(e) => { if (e.key === 'Enter') commitName(); if (e.key === 'Escape') { editingName = false; } }}
      class="w-full text-sm font-semibold text-warm-text bg-warm-panel border border-warm-border rounded px-2 py-1 focus:outline-none focus:border-[#C07A3A]"
    />
  {:else}
    <button
      onclick={() => { nameInput = fileStore.pipelineMeta.name; editingName = true; }}
      class="w-full text-left text-sm font-semibold text-warm-text hover:text-[#C07A3A] transition-colors truncate"
      title="Click to rename"
    >
      {fileStore.pipelineMeta.name}
      {#if fileStore.isDirty}
        <span class="text-warm-muted">•</span>
      {/if}
    </button>
  {/if}
  {#if fileStore.currentFilePath}
    <div class="text-[10px] text-warm-muted truncate" title={fileStore.currentFilePath}>
      {fileStore.currentFilePath}
    </div>
  {:else}
    <div class="text-[10px] text-warm-muted italic">Not saved</div>
  {/if}
</div>
