<script lang="ts">
  import { Handle, Position } from '@xyflow/svelte';
  
  let { data } = $props();
  
  let host = $state(data.config?.host || 'localhost');
  let port = $state(data.config?.port || 3306);
  let database = $state(data.config?.database || '');
  let user = $state(data.config?.user || '');
  let password = $state(data.config?.password || '');
  let query = $state(data.config?.query || 'SELECT * FROM table');

  function updateConfig() {
    data.config = { host, port, database, user, password, query };
  }
</script>

<div class="bg-white border-l-4 border-l-[#4A7C59] border border-warm-border rounded shadow-sm w-80">
  <div class="px-3 py-2 border-b border-warm-border flex items-center gap-2 bg-warm-bg rounded-t">
    <span class="text-xl">🐬</span>
    <span class="font-bold text-warm-text text-sm">MySQL Source</span>
  </div>

  <div class="p-3 flex flex-col gap-3 relative">
    <div class="grid grid-cols-3 gap-2">
      <div class="col-span-2">
        <label class="text-xs text-warm-sub font-medium">Host</label>
        <input 
          type="text" 
          bind:value={host} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
      <div>
        <label class="text-xs text-warm-sub font-medium">Port</label>
        <input 
          type="number" 
          bind:value={port} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
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
        <label class="text-xs text-warm-sub font-medium">User</label>
        <input 
          type="text" 
          bind:value={user} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
    </div>

    <div>
      <label class="text-xs text-warm-sub font-medium">Password</label>
      <input 
        type="password" 
        bind:value={password} 
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
      />
    </div>

    <div>
      <label class="text-xs text-warm-sub font-medium">SQL Query</label>
      <textarea 
        bind:value={query} 
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none h-20 font-mono"
      ></textarea>
    </div>

    <Handle type="source" position={Position.Right} class="!bg-[#4A7C59] !w-3 !h-3 !-right-1.5" />
  </div>
</div>

