<script lang="ts">
  let { dtype } = $props<{ dtype: string }>();

  function classesFor(t: string) {
    const x = (t || '').toLowerCase();
    if (x.includes('int')) return 'bg-[#FDE9C8] text-warm-text border-warm-border';
    if (x.includes('float') || x.includes('decimal')) return 'bg-[#FDE9C8] text-warm-text border-warm-border';
    if (x.includes('date') || x.includes('time')) return 'bg-[#DDEAF2] text-warm-text border-warm-border';
    if (x.includes('bool')) return 'bg-[#E3F1E7] text-warm-text border-warm-border';
    if (x.includes('utf') || x.includes('string') || x.includes('text')) return 'bg-warm-panel text-warm-text border-warm-border';
    return 'bg-warm-panel text-warm-text border-warm-border';
  }
</script>

<span class={"inline-flex items-center px-2 py-0.5 text-[11px] border rounded " + classesFor(dtype)}>
  {dtype}
</span>

