export type ReviewIdentity = { documentId: string; revision: number };
type LoadTicket = { documentId: string; selection: number; request: number };
type MutationTicket = ReviewIdentity & { selection: number; token: number };

/** Synchronous identity fences complement React's asynchronous state updates. */
export class ReviewerSession {
  selectedId: string | null = null;
  loading = false;
  private selection = 0;
  private request = 0;
  private token = 0;
  private identity: ReviewIdentity | null = null;
  private mutation: MutationTicket | null = null;

  select(documentId: string | null) {
    this.selectedId = documentId;
    this.selection++;
    this.request++;
    this.identity = null;
    this.loading = Boolean(documentId);
  }

  startLoad(documentId: string): LoadTicket | null {
    if (this.selectedId !== documentId) return null;
    this.loading = true;
    this.identity = null;
    return { documentId, selection: this.selection, request: ++this.request };
  }

  isCurrent(ticket: LoadTicket) {
    return ticket.documentId === this.selectedId && ticket.selection === this.selection && ticket.request === this.request;
  }

  acceptLoad(ticket: LoadTicket, response: { id: string; documentRevision: number }) {
    if (!this.isCurrent(ticket)) return false;
    if (response.id !== ticket.documentId || !Number.isSafeInteger(response.documentRevision) || response.documentRevision < 1) {
      throw new Error("The loaded document identity is inconsistent. Reload before reviewing.");
    }
    this.identity = { documentId: response.id, revision: response.documentRevision };
    this.loading = false;
    return true;
  }

  failLoad(ticket: LoadTicket) {
    if (!this.isCurrent(ticket)) return false;
    this.loading = false;
    this.identity = null;
    return true;
  }

  invalidateReview() {
    this.request++;
    this.identity = null;
    this.loading = false;
  }

  canMutate(form: ReviewIdentity | null) {
    return Boolean(form && !this.loading && !this.mutation && this.matches(form));
  }

  private matches(form: ReviewIdentity) {
    return form.documentId === this.selectedId && form.documentId === this.identity?.documentId && form.revision === this.identity?.revision;
  }

  beginMutation(form: ReviewIdentity): MutationTicket | null {
    if (!this.canMutate(form)) return null;
    this.mutation = { ...form, selection: this.selection, token: ++this.token };
    return this.mutation;
  }

  finishMutation(ticket: MutationTicket) {
    if (this.mutation?.token !== ticket.token) return false;
    this.mutation = null;
    // A -> B -> A is a new review session, even at the same revision.
    return ticket.selection === this.selection && this.matches(ticket) && !this.loading;
  }
}
