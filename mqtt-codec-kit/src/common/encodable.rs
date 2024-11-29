use std::{
    convert::Infallible,
    error::Error,
    fmt::Display,
    io::{self, Read, Write},
    marker::Sized,
    slice,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub trait Encodable {
    /// Encodes to writer
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()>;
    /// Length of bytes after encoded
    fn encoded_length(&self) -> u32;
}

impl<T: Encodable> Encodable for Option<T> {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        if let Some(this) = self {
            this.encode(writer)?
        }
        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.as_ref().map_or(0, |x| x.encoded_length())
    }
}

impl Encodable for &str {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        assert!(self.as_bytes().len() <= u16::MAX as usize);

        writer
            .write_u16::<BigEndian>(self.as_bytes().len() as u16)
            .and_then(|_| writer.write_all(self.as_bytes()))
    }

    fn encoded_length(&self) -> u32 {
        2 + self.as_bytes().len() as u32
    }
}

impl Encodable for &[u8] {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_all(self)
    }

    fn encoded_length(&self) -> u32 {
        self.len() as u32
    }
}

impl Encodable for String {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        (&self[..]).encode(writer)
    }

    fn encoded_length(&self) -> u32 {
        (&self[..]).encoded_length()
    }
}

impl Encodable for Vec<u8> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        (&self[..]).encode(writer)
    }

    fn encoded_length(&self) -> u32 {
        (&self[..]).encoded_length()
    }
}

impl Encodable for () {
    fn encode<W: Write>(&self, _: &mut W) -> Result<(), io::Error> {
        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        0
    }
}

/// Methods for decoding bytes to an Object according to MQTT specification
pub trait Decodable: Sized {
    type Error: Error;
    type Cond;

    /// Decodes object from reader
    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error>
    where
        Self::Cond: Default,
    {
        Self::decode_with(reader, Default::default())
    }

    /// Decodes object with additional data (or hints)
    fn decode_with<R: Read>(reader: &mut R, cond: Self::Cond) -> Result<Self, Self::Error>;
}

impl Decodable for String {
    type Error = io::Error;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<String, io::Error> {
        let VarBytes(buf) = VarBytes::decode(reader)?;

        String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

impl Decodable for Vec<u8> {
    type Error = io::Error;
    type Cond = Option<u32>;

    fn decode_with<R: Read>(reader: &mut R, length: Option<u32>) -> Result<Self, Self::Error> {
        match length {
            Some(length) => {
                let mut buf = Vec::with_capacity(length as usize);
                reader.take(length.into()).read_to_end(&mut buf)?;
                Ok(buf)
            }
            None => {
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf)?;
                Ok(buf)
            }
        }
    }
}

impl Decodable for () {
    type Error = Infallible;
    type Cond = ();

    fn decode_with<R: Read>(_: &mut R, _: ()) -> Result<Self, Self::Error> {
        Ok(())
    }
}

/// Bytes that encoded with length
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct VarBytes(pub Vec<u8>);

impl Encodable for VarBytes {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        assert!(self.0.len() <= u16::MAX as usize);
        let len = self.0.len() as u16;
        writer.write_u16::<BigEndian>(len)?;
        writer.write_all(&self.0)?;
        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        2 + self.0.len() as u32
    }
}

impl Decodable for VarBytes {
    type Error = io::Error;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _: ()) -> Result<Self, Self::Error> {
        let length = reader.read_u16::<BigEndian>()?;
        let mut buf = Vec::with_capacity(length as usize);
        reader.take(length.into()).read_to_end(&mut buf)?;
        Ok(Self(buf))
    }
}

impl Display for VarBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) if s.chars().all(|c| c.is_ascii_graphic() || c == ' ') => {
                write!(f, "{}", s)
            }
            _ => {
                write!(f, "[")?;
                let mut iter = self.0.iter();
                if let Some(first) = iter.next() {
                    write!(f, "{}", first)?;
                    for byte in iter {
                        write!(f, ", {}", byte)?;
                    }
                }
                write!(f, "]")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VarInt(pub u32);

impl Encodable for VarInt {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut value = self.0;
        loop {
            let mut byte = (value % 128) as u8;
            value /= 128;
            if value > 0 {
                byte |= 128;
            }
            writer.write_u8(byte)?;
            if value == 0 {
                break;
            }
        }
        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        if self.0 >= 2_097_152 {
            4
        } else if self.0 >= 16_384 {
            3
        } else if self.0 >= 128 {
            2
        } else {
            1
        }
    }
}

impl Decodable for VarInt {
    type Error = io::Error;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _cond: Self::Cond) -> Result<Self, Self::Error> {
        let mut byte = 0u8;
        let mut var_int: u32 = 0;
        let mut i: usize = 0;
        loop {
            reader.read_exact(slice::from_mut(&mut byte))?;
            var_int |= (u32::from(byte) & 0x7F) << (7 * i);
            if byte & 0x80 == 0 {
                break;
            } else if i < 3 {
                i += 1;
            } else {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
        }
        Ok(Self(var_int))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Cursor;

    #[test]
    fn varbyte_encode() {
        let test_var = vec![0, 1, 2, 3, 4, 5];
        let bytes = VarBytes(test_var);

        assert_eq!(bytes.encoded_length() as usize, 2 + 6);

        let mut buf = Vec::new();
        bytes.encode(&mut buf).unwrap();

        assert_eq!(&buf, &[0, 6, 0, 1, 2, 3, 4, 5]);

        let mut reader = Cursor::new(buf);
        let decoded = VarBytes::decode(&mut reader).unwrap();

        assert_eq!(decoded, bytes);
    }
}
