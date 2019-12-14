This is a fork of
[sled-extensions](https://git.asonix.dog/Aardwolf/sled-extensions)

# Sled Extensions
_Wrappers around the [Sled embedded database](https://docs.rs/sled/0.28.0/sled) to permit
storing structured data_

- [crates.io](https://crates.io/crates/sled-extensions)
- [docs.rs](https://docs.rs/sled-extensions)
- [Join the discussion on Matrix](https://matrix.to/#/!skqvSdiKcFwIdaQoLD:asonix.dog?via=asonix.dog)

Using Sled Extensions is much like using Sled. The Tree API mirrors Sled's directly, and the
[`Db`] type is extended through traits.

```rust
use sled_extensions::{Config, DbExt};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Config::default().temporary(true).open()?;
    let tree = db.open_json_tree::<usize>("json-tree")?;

    tree.insert(b"hey", 32)?;

    if let Some(num) = tree.get(b"hey")? {
        assert_eq!(num, 32);
    } else {
        unreachable!("Shouldn't be empty");
    }

    Ok(())
}
```

Available features
- `bincode` - Enable storing bincode-encoded data
- `cbor` - Enable storing cbor-encoded data
- `json` - Enable storing json-encoded data

### Contributing
Unless otherwise stated, all contributions to this project will be licensed under the CSL with
the exceptions listed in the License section of this file.

### License
This work is licensed under the Cooperative Software License. This is not a Free Software
License, but may be considered a "source-available License." For most hobbyists, self-employed
developers, worker-owned companies, and cooperatives, this software can be used in most
projects so long as this software is distributed under the terms of the CSL. For more
information, see the provided LICENSE file. If none exists, the license can be found online
[here](https://lynnesbian.space/csl/). If you are a free software project and wish to use this
software under the terms of the GNU Affero General Public License, please contact me at
[asonix@asonix.dog](mailto:asonix@asonix.dog) and we can sort that out. If you wish to use this
project under any other license, especially in proprietary software, the answer is likely no.
