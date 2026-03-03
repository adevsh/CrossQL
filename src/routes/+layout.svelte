<script>
  import "../app.css";
  let { children } = $props();
</script>

<div class="min-h-screen bg-warm-bg text-warm-text font-sans antialiased">
  {@render children()}
</div>
