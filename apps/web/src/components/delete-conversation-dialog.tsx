"use client";

import { useEffect, useRef, type RefObject } from "react";
import { Trash2 } from "lucide-react";

export function DeleteConversationDialog({ title, onCancel, onDelete, fallbackFocus }: {
  title: string;
  onCancel: () => void;
  onDelete: () => void;
  fallbackFocus: RefObject<HTMLButtonElement | null>;
}) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const cancelRef = useRef<HTMLButtonElement>(null);
  const deleteRef = useRef<HTMLButtonElement>(null);
  const backdropPress = useRef(false);

  useEffect(() => {
    const dialog = dialogRef.current!;
    const trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const previousOverflow = document.body.style.overflow;
    dialog.showModal(); // Native modal focus containment and inert background.
    cancelRef.current?.focus(); // The safe action is first, never the destructive action.
    document.body.style.overflow = "hidden";
    return () => {
      dialog.close();
      document.body.style.overflow = previousOverflow;
      // Deletion or another tab can remove the opener. Return to a stable control.
      queueMicrotask(() => (trigger?.isConnected ? trigger : fallbackFocus.current)?.focus());
    };
  }, [fallbackFocus]);

  function outside(event: { clientX: number; clientY: number }) {
    const rect = dialogRef.current!.getBoundingClientRect();
    return event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom;
  }

  return <dialog ref={dialogRef} className="app-confirm-modal"
    aria-labelledby="delete-conversation-title" aria-describedby="delete-conversation-description"
    onKeyDown={event => {
      if (event.key !== "Tab" || event.ctrlKey || event.metaKey || event.altKey) return;
      const first = cancelRef.current, last = deleteRef.current;
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
    }}
    onCancel={event => { event.preventDefault(); onCancel(); }}
    onPointerDown={event => { backdropPress.current = event.target === event.currentTarget && outside(event); }}
    onClick={event => { if (backdropPress.current && event.target === event.currentTarget && outside(event)) onCancel(); backdropPress.current = false; }}>
    <div className="app-confirm-modal__icon" aria-hidden="true"><Trash2 /></div>
    <div>
      <p className="app-confirm-modal__eyebrow">Delete conversation</p>
      <h2 id="delete-conversation-title" className="app-confirm-modal__title">Remove this chat?</h2>
      <p id="delete-conversation-description" className="app-confirm-modal__copy">
        This removes "{title}" from active chat history on this browser. Original legacy recovery data is retained separately; clear site data to remove all local copies.
      </p>
    </div>
    <div className="app-confirm-modal__actions">
      <button ref={cancelRef} type="button" className="app-confirm-modal__button" onClick={onCancel}>Cancel</button>
      <button ref={deleteRef} type="button" className="app-confirm-modal__button app-confirm-modal__button--danger" onClick={onDelete}>Delete</button>
    </div>
  </dialog>;
}
