<script lang="ts">
  import { onMount } from 'svelte';
  import { createToastViewModel, setupAutoDismiss } from '$lib/components/toastLogic';

  let {
    message,
    kind = 'info',
    linkText,
    href,
    onDismiss
  }: {
    message: string;
    kind?: 'success' | 'error' | 'info';
    linkText?: string;
    href?: string;
    onDismiss: () => void;
  } = $props();

  onMount(() => {
    return setupAutoDismiss(onDismiss, 4000);
  });

  const view = $derived(createToastViewModel(message, linkText, href));
</script>

<div
  role="status"
  class="pointer-events-auto rounded border px-3 py-2 text-xs shadow-sm bg-white flex items-center gap-2 min-w-64 max-w-[26rem]"
  class:border-warm-border={kind === 'info'}
  class:border-[#4A7C59]={kind === 'success'}
  class:border-[#B85C4A]={kind === 'error'}
>
  <div
    class="h-2 w-2 rounded-full"
    class:bg-warm-muted={kind === 'info'}
    class:bg-[#4A7C59]={kind === 'success'}
    class:bg-[#B85C4A]={kind === 'error'}
  ></div>
  <div class="text-warm-text flex-1">{view.message}</div>
  {#if view.hasLink}
    <a class="text-warm-sub hover:text-warm-text underline" href={view.href}>{view.linkText}</a>
  {/if}
  <button
    type="button"
    onclick={onDismiss}
    class="text-warm-muted hover:text-warm-text"
    aria-label="Dismiss notification"
  >×</button>
</div>
