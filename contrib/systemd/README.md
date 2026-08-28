# netsukuku-rs under systemd

`ntkd.service` and `ntkd.toml` are the unit and default config the Arch packages install to
`/usr/lib/systemd/system/` and `/etc/ntkd/`. They live here, beside the code, rather than in the
packaging repository, because both encode facts about the daemon that only this tree can keep
true — see "Why these live here" below.

## Installing by hand

```sh
install -Dm644 contrib/systemd/ntkd.service /usr/lib/systemd/system/ntkd.service
install -Dm644 contrib/systemd/ntkd.toml    /etc/ntkd/ntkd.toml
$EDITOR /etc/ntkd/ntkd.toml     # nics is mandatory and deliberately unset
systemctl enable --now ntkd
```

The unit ships `disabled`. Enabling it before setting `nics` is safe — the daemon refuses to start
and names the file it wants edited:

```
ntkd: error: failed to load config /etc/ntkd/ntkd.toml: ... missing field `nics`
```

## What the unit grants, and why

`DynamicUser=yes` means `AmbientCapabilities` is an explicit allow-list with no implicit default,
so every capability the daemon needs has to be named:

| Capability | Needed for |
|---|---|
| `CAP_NET_ADMIN` | programming kernel routes, addresses and rules over rtnetlink |
| `CAP_NET_RAW` | the neighbourhood liveness probe's raw-ICMP fallback |
| `CAP_NET_BIND_SERVICE` | binding port 269, which is privileged |

All three appear in `CapabilityBoundingSet` as well as `AmbientCapabilities`: a bounding set that
omitted one would silently strip the ambient grant.

`StartLimitIntervalSec=60` / `StartLimitBurst=5` bound the restart loop. `Restart=on-failure` with
`RestartSec=3` never trips systemd's default limit of 5 starts per 10 s, because 3 s spacing only
fits about four — so a permanent misconfiguration used to respawn indefinitely instead of parking
in `failed` where `systemctl status` can explain it. After fixing the config, clear the limiter
with `systemctl reset-failed ntkd`.

`--status-socket /run/ntkd/ntkd.sock` overrides the CLI default of `/tmp/ntkd.sock`, which
`PrivateTmp=yes` would make unreachable to an interactive `ntkd status`. `/run/ntkd` is
`RuntimeDirectory` and mode `0750`, owned by the dynamic user, so reading it needs `sudo`.

## Why these live here

Both files depend on details this repository defines and can change:

- The capability list follows from where the daemon binds and what it calls. Port 269 being
  privileged is why `CAP_NET_BIND_SERVICE` is present at all.
- `ntkd.toml`'s comments quote the daemon's own error text, name real config fields, and explain
  why `nics` has no safe default and why `require_auth` must stay `false` while ANDNA registration
  is enforced regardless.
- `StateDirectory=ntkd` is what makes `/var/lib/ntkd` the right place for `andna_key_path` and
  `node_key_path`, which the config comments point at.

A packaging repository that carried its own copies would drift from all of that silently, and did:
the unit shipped without `CAP_NET_BIND_SERVICE` for three releases, so no packaged install could
bind the default port. The packages now install these files straight from the release, which makes
this directory the single source of truth.

## OpenWrt

`contrib/openwrt/` is separate and does not use these files: procd has no unit concept, and its
init script renders TOML from UCI rather than shipping a config to edit.
