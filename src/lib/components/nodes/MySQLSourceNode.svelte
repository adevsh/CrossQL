<script lang="ts">
  import { Handle, Position, useSvelteFlow } from '@xyflow/svelte';
  
  let { id, data } = $props();
  const { deleteElements } = useSvelteFlow();
  
  let host = $state('localhost');
  let port = $state(3306);
  let database = $state('');
  let user = $state('');
  let password = $state('');
  let query = $state('SELECT * FROM table');

  function statusRingClass() {
    const s = data?.run_state;
    if (s === 'running') return 'border-accent animate-pulse bg-accent-light';
    if (s === 'done') return 'border-[#4A7C59] bg-[#4A7C59]';
    if (s === 'error') return 'border-[#B85C4A] bg-[#B85C4A]';
    return 'border-warm-border bg-white';
  }

  function updateConfig() {
    data.config = { host, port, database, user, password, query };
  }

  function stopFlowEvents(e: Event) {
    e.stopPropagation();
  }

  async function removeNode(e: Event) {
    e.stopPropagation();
    await deleteElements({ nodes: [{ id }] });
  }

  $effect(() => {
    const cfg = data?.config ?? {};
    const nextHost = typeof cfg.host === 'string' ? cfg.host : 'localhost';
    const nextPort = typeof cfg.port === 'number' ? cfg.port : 3306;
    const nextDatabase = typeof cfg.database === 'string' ? cfg.database : '';
    const nextUser = typeof cfg.user === 'string' ? cfg.user : '';
    const nextPassword = typeof cfg.password === 'string' ? cfg.password : '';
    const nextQuery = typeof cfg.query === 'string' ? cfg.query : 'SELECT * FROM table';
    if (host !== nextHost) host = nextHost;
    if (port !== nextPort) port = nextPort;
    if (database !== nextDatabase) database = nextDatabase;
    if (user !== nextUser) user = nextUser;
    if (password !== nextPassword) password = nextPassword;
    if (query !== nextQuery) query = nextQuery;
  });
</script>

<div class="bg-white border-l-4 border-l-[#4A7C59] border border-warm-border rounded shadow-sm w-80">
  <div class="px-3 py-2 border-b border-warm-border flex items-center justify-between bg-warm-bg rounded-t">
    <div class="flex items-center gap-2">
      <span class={"w-3 h-3 rounded-full border-2 " + statusRingClass()}></span>
      <span class="text-xl">🐬</span>
      <span class="font-bold text-warm-text text-sm">MySQL Source</span>
    </div>
    <button
      type="button"
      onclick={removeNode}
      onpointerdown={stopFlowEvents}
      class="nodrag w-6 h-6 flex items-center justify-center rounded hover:bg-warm-light text-warm-sub"
      aria-label="Remove node"
      title="Remove"
    >
      ×
    </button>
  </div>

  <div class="nodrag p-3 flex flex-col gap-3 relative">
    <div class="grid grid-cols-3 gap-2">
      <div class="col-span-2">
        <label for="mysql-host-{id}" class="text-xs text-warm-sub font-medium">Host</label>
        <input 
          id="mysql-host-{id}"
          type="text" 
          bind:value={host} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
      <div>
        <label for="mysql-port-{id}" class="text-xs text-warm-sub font-medium">Port</label>
        <input 
          id="mysql-port-{id}"
          type="number" 
          bind:value={port} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
    </div>

    <div class="grid grid-cols-2 gap-2">
      <div>
        <label for="mysql-database-{id}" class="text-xs text-warm-sub font-medium">Database</label>
        <input 
          id="mysql-database-{id}"
          type="text" 
          bind:value={database} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
      <div>
        <label for="mysql-user-{id}" class="text-xs text-warm-sub font-medium">User</label>
        <input 
          id="mysql-user-{id}"
          type="text" 
          bind:value={user} 
          oninput={updateConfig}
          class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
        />
      </div>
    </div>

    <div>
      <label for="mysql-password-{id}" class="text-xs text-warm-sub font-medium">Password</label>
      <input 
        id="mysql-password-{id}"
        type="password" 
        bind:value={password} 
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none"
      />
    </div>

    <div>
      <label for="mysql-query-{id}" class="text-xs text-warm-sub font-medium">SQL Query</label>
      <textarea 
        id="mysql-query-{id}"
        bind:value={query} 
        oninput={updateConfig}
        class="w-full text-xs px-2 py-1 border border-warm-border rounded focus:border-accent outline-none h-20 font-mono"
      ></textarea>
    </div>
    {#if data?.stats?.rows_out !== undefined}
      <div class="text-[11px] text-warm-sub">Rows out: {data.stats.rows_out}</div>
    {/if}


    <Handle type="source" position={Position.Right} class="!bg-[#4A7C59] !w-3 !h-3 !-right-1.5" />
  </div>
</div>
