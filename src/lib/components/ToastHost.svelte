<script lang="ts">
  import Toast from '$lib/components/Toast.svelte';
  import { toastStore } from '$lib/stores/toastStore';
</script>

<div class="fixed top-3 right-3 z-50 flex flex-col gap-2 pointer-events-none">
  {#each $toastStore as toast (toast.id)}
    <Toast
      message={toast.message}
      kind={toast.kind}
      linkText={toast.linkText}
      href={toast.href}
      onDismiss={() => toastStore.dismiss(toast.id)}
    />
  {/each}
</div>
