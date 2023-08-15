# amp-ext
this module will extract data from your amplitude project, retrying when necessary, and dump json (or json.gz) files onto disk.

this module can be used in combination with [`amp-to-mp`](https://github.com/ak--47/amp-to-mp) to do a full historical migration

it is implemented as a CLI and requires [Node.js](https://nodejs.org/en/download).

## usage
```bash
npx amp-ext --key foo --secret bar --start 2022-04-20 --end 2023-04-20
```

### help / options:
```bash
npx amp-ext --help
```
