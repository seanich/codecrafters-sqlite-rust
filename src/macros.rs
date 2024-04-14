#[macro_export]
macro_rules! field_decoder {
    ($type:ty; $name:ident) => {
        pub fn $name(&self) -> $type {
            <$type>::from_be_bytes(self.$name)
        }
    };
}
