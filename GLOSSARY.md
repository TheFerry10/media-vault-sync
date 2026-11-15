# Media Archive

This context preserves photos and videos from multiple origins in a browsable archive organized by capture time while retaining provenance.

## Language

**Source**:
A named origin category assigned to imported media, such as Phone-1, Phone-2, or SD-Cards. It preserves where an item came from.
_Avoid_: device, location

**Inbox**:
A source-specific holding area for newly received media before it is organized.
_Avoid_: staging

**Staging**:
A temporary local collection of organized media awaiting verified transfer to the cloud archive.
_Avoid_: local archive

**Cloud archive**:
The durable Azure Files collection of organized media. A media item joins the archive only after verification and publication; archived items are append-only and are not overwritten or deleted. Operational state is stored separately and may be updated.
_Avoid_: sync, sync target, mirror

**Capture time**:
The best available wall-clock time at which a media item was recorded, used for chronological organization. Prefer valid embedded capture metadata, then a recognized filename date, then filesystem modification time; interpret source times as recorded and use the Ubuntu machine's timezone for the filesystem fallback.
_Avoid_: import time, modification time

**Duplicate media**:
Media with the same content from the same source. The same content imported from different sources remains a separate archive item to preserve provenance; a confirmed re-imported local copy can be removed after the prior archive item is verified.
_Avoid_: redundant copy

**Verified upload**:
Evidence that an item has arrived intact in the cloud archive. A local staged copy is eligible for cleanup only after its upload is verified.
_Avoid_: completed transfer

**Pending upload**:
A media item being transferred or checked in a separate cloud path. It is not part of the Cloud archive until verification and publication succeed.
_Avoid_: archived item

**Failed item**:
Media that cannot be processed and remains in its source Inbox with the reason recorded until it is explicitly requeued.
_Avoid_: quarantined file, discarded file

**Sidecar**:
A non-media file associated with a media item, such as an edit or metadata companion. A sidecar is related to a media item only when it has the same source and original filename stem.
_Avoid_: unrelated file
