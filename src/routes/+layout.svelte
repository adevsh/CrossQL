<script>
  import "../app.css";
  import { page } from '$app/stores';
  import ToastHost from '$lib/components/ToastHost.svelte';
  let { children } = $props();
</script>

<div class="min-h-screen bg-warm-bg text-warm-text font-sans antialiased">
  <header class="border-b border-warm-border bg-warm-panel px-4 py-2">
    <nav class="flex items-center gap-2">
      <a
        href="/"
        class="px-3 py-1.5 text-sm rounded border transition-colors"
        class:border-[#C07A3A]={$page.url.pathname === '/'}
        class:bg-white={$page.url.pathname === '/'}
        class:text-warm-text={$page.url.pathname === '/'}
        class:border-warm-border={$page.url.pathname !== '/'}
        class:text-warm-sub={$page.url.pathname !== '/'}
      >Pipeline Editor</a>
      <a
        href="/remote"
        class="px-3 py-1.5 text-sm rounded border transition-colors"
        class:border-[#C07A3A]={$page.url.pathname.startsWith('/remote')}
        class:bg-white={$page.url.pathname.startsWith('/remote')}
        class:text-warm-text={$page.url.pathname.startsWith('/remote')}
        class:border-warm-border={!$page.url.pathname.startsWith('/remote')}
        class:text-warm-sub={!$page.url.pathname.startsWith('/remote')}
      >Remote Execution</a>
    </nav>
  </header>
  {@render children()}
  <ToastHost />
</div>
