//! (macrdp divergence 1) Server-to-client request envelopes.
//!
//! Upstream's own `RdpdrServer` frames device I/O requests internally (its `DriveRequestBody` is
//! private), so a server that keeps its own RDPDR processor — macrdp's, in the vendored
//! `ironrdp-server` — has no public way to turn a request into an `SvcMessage`. This module adds
//! the envelope macrdp sends:
//!
//! - `impl Encode + SvcEncode for ServerDriveIoRequest`: `PAKID_CORE_DEVICE_IOREQUEST` header + the
//!   request's own (upstream) encoding.
//!
//! Everything here is additive; no upstream item is modified.

use ironrdp_core::{Encode, EncodeResult, WriteCursor, ensure_size, unsupported_value_err};
use ironrdp_svc::SvcEncode;

use super::efs::ServerDriveIoRequest;
use super::{Component, PacketId, SharedHeader};

const DEVICE_IO_REQUEST_HEADER: SharedHeader = SharedHeader {
    component: Component::RdpdrCtypCore,
    packet_id: PacketId::CoreDeviceIoRequest,
};

/// The body of a drive request: everything after the `SharedHeader`, including its `DeviceIoRequest`.
trait RequestBody {
    fn encode_body(&self, dst: &mut WriteCursor<'_>) -> EncodeResult<()>;
    fn body_size(&self) -> usize;
}

macro_rules! impl_request_body {
    ($($ty:ident),+ $(,)?) => {
        $(impl RequestBody for super::efs::$ty {
            fn encode_body(&self, dst: &mut WriteCursor<'_>) -> EncodeResult<()> {
                self.encode(dst)
            }

            fn body_size(&self) -> usize {
                self.size()
            }
        })+
    };
}

impl_request_body!(
    DeviceCreateRequest,
    ServerDriveQueryInformationRequest,
    DeviceCloseRequest,
    ServerDriveQueryDirectoryRequest,
    ServerDriveNotifyChangeDirectoryRequest,
    ServerDriveQueryVolumeInformationRequest,
    DeviceReadRequest,
    DeviceWriteRequest,
    DeviceFlushBuffersRequest,
    ServerDriveSetInformationRequest,
    ServerDriveLockControlRequest,
    ServerDriveQuerySecurityRequest,
    ServerDriveSetSecurityRequest,
);

impl ServerDriveIoRequest {
    /// `None` for `DeviceControlRequest`, which doesn't carry its input buffer and so can't be
    /// sent as-is (upstream's server excludes it for the same reason). Exhaustive on purpose: a
    /// new upstream variant fails to compile here instead of silently becoming unencodable.
    fn body(&self) -> Option<&dyn RequestBody> {
        match self {
            Self::ServerCreateDriveRequest(r) => Some(r),
            Self::ServerDriveQueryInformationRequest(r) => Some(r),
            Self::DeviceCloseRequest(r) => Some(r),
            Self::ServerDriveQueryDirectoryRequest(r) => Some(r),
            Self::ServerDriveNotifyChangeDirectoryRequest(r) => Some(r),
            Self::ServerDriveQueryVolumeInformationRequest(r) => Some(r),
            Self::DeviceReadRequest(r) => Some(r),
            Self::DeviceWriteRequest(r) => Some(r),
            Self::DeviceFlushBuffersRequest(r) => Some(r),
            Self::ServerDriveSetInformationRequest(r) => Some(r),
            Self::ServerDriveLockControlRequest(r) => Some(r),
            Self::ServerDriveQuerySecurityRequest(r) => Some(r),
            Self::ServerDriveSetSecurityRequest(r) => Some(r),
            Self::DeviceControlRequest(_) => None,
        }
    }
}

impl Encode for ServerDriveIoRequest {
    fn encode(&self, dst: &mut WriteCursor<'_>) -> EncodeResult<()> {
        let Some(body) = self.body() else {
            return Err(unsupported_value_err!(
                "ServerDriveIoRequest::encode",
                "ServerDriveIoRequest",
                "DeviceControlRequest (no input buffer)".to_owned()
            ));
        };
        ensure_size!(ctx: self.name(), in: dst, size: self.size());
        DEVICE_IO_REQUEST_HEADER.encode(dst)?;
        body.encode_body(dst)
    }

    fn name(&self) -> &'static str {
        "DR_DEVICE_IOREQUEST"
    }

    fn size(&self) -> usize {
        SharedHeader::SIZE + self.body().map_or(0, RequestBody::body_size)
    }
}

impl SvcEncode for ServerDriveIoRequest {}

#[cfg(test)]
mod tests {
    use ironrdp_core::{ReadCursor, encode_vec};

    use super::*;
    use crate::pdu::efs::{
        CreateDisposition, CreateOptions, DesiredAccess, DeviceCloseRequest, DeviceCreateRequest, DeviceIoRequest,
        DeviceReadRequest, FileAttributes, MajorFunction, MinorFunction, SharedAccess,
    };

    fn io_header(major_function: MajorFunction) -> DeviceIoRequest {
        DeviceIoRequest {
            device_id: 0x07,
            file_id: 0x11,
            completion_id: 0x42,
            major_function,
            minor_function: MinorFunction::from(0),
        }
    }

    /// Encodes a drive request and parses it back through the client's decode chain, which must
    /// consume everything but `unread_padding` bytes.
    fn drive_roundtrip(req: ServerDriveIoRequest, unread_padding: usize) -> ServerDriveIoRequest {
        let bytes = encode_vec(&req).unwrap();
        assert_eq!(bytes.len(), req.size());
        let mut src = ReadCursor::new(&bytes);
        let header = SharedHeader::decode(&mut src).unwrap();
        assert!(matches!(header.component, Component::RdpdrCtypCore));
        assert_eq!(header.packet_id, PacketId::CoreDeviceIoRequest);
        let dev_io = DeviceIoRequest::decode(&mut src).unwrap();
        let decoded = ServerDriveIoRequest::decode(dev_io, &mut src).unwrap();
        assert_eq!(src.len(), unread_padding, "unexpected trailing bytes");
        decoded
    }

    #[test]
    fn drive_create_request_roundtrip() {
        let req = ServerDriveIoRequest::ServerCreateDriveRequest(DeviceCreateRequest {
            device_io_request: io_header(MajorFunction::Create),
            desired_access: DesiredAccess::FILE_READ_DATA_OR_FILE_LIST_DIRECTORY | DesiredAccess::SYNCHRONIZE,
            allocation_size: 0,
            file_attributes: FileAttributes::empty(),
            shared_access: SharedAccess::all(),
            create_disposition: CreateDisposition::FILE_OPEN,
            create_options: CreateOptions::FILE_NON_DIRECTORY_FILE,
            path: "dir\\file.txt".to_owned(),
        });
        assert_eq!(drive_roundtrip(req.clone(), 0), req);
    }

    #[test]
    fn drive_read_and_close_requests_roundtrip() {
        let read = ServerDriveIoRequest::DeviceReadRequest(DeviceReadRequest {
            device_io_request: io_header(MajorFunction::Read),
            length: 262_144,
            offset: 1 << 33,
        });
        assert_eq!(drive_roundtrip(read.clone(), 0), read);

        let close = ServerDriveIoRequest::DeviceCloseRequest(DeviceCloseRequest {
            device_io_request: io_header(MajorFunction::Close),
        });
        // Upstream's decode leaves DR_CLOSE_REQ's 32 bytes of padding unread; encode writes them.
        assert_eq!(drive_roundtrip(close.clone(), 32), close);
    }
}
