import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { createToastViewModel, setupAutoDismiss, shouldAutoSwitchTabOnMount } from './toastLogic';

describe('Toast component', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('renders with the provided message', () => {
    const view = createToastViewModel('Pipeline sent');
    expect(view.message).toBe('Pipeline sent');
  });

  it('auto-dismisses after 4 seconds', async () => {
    const onDismiss = vi.fn();
    setupAutoDismiss(onDismiss, 4000);
    await vi.advanceTimersByTimeAsync(4000);
    expect(onDismiss).toHaveBeenCalledTimes(1);
  });

  it('does not auto-switch tab when View in Remote link is present', () => {
    const view = createToastViewModel('Pipeline sent', 'View in Remote →', '/remote');
    expect(view.hasLink).toBe(true);
    expect(shouldAutoSwitchTabOnMount()).toBe(false);
  });
});
