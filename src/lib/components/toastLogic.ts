export function createToastViewModel(message: string, linkText?: string, href?: string) {
  return {
    message,
    hasLink: Boolean(linkText && href),
    linkText,
    href
  };
}

export function setupAutoDismiss(onDismiss: () => void, timeoutMs = 4000) {
  const timer = setTimeout(() => onDismiss(), timeoutMs);
  return () => clearTimeout(timer);
}

export function shouldAutoSwitchTabOnMount() {
  return false;
}
