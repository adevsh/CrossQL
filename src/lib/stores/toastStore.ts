import { writable } from 'svelte/store';

export type ToastItem = {
  id: string;
  message: string;
  kind: 'success' | 'error' | 'info';
  linkText?: string;
  href?: string;
};

function createToastStore() {
  const { subscribe, update } = writable<ToastItem[]>([]);

  function push(item: Omit<ToastItem, 'id'>) {
    const id = crypto.randomUUID();
    update((list) => [...list, { id, ...item }]);
    return id;
  }

  function dismiss(id: string) {
    update((list) => list.filter((toast) => toast.id !== id));
  }

  function clear() {
    update(() => []);
  }

  return { subscribe, push, dismiss, clear };
}

export const toastStore = createToastStore();
