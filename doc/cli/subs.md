# The `corrosion subs` command

Command to view subscriptions.

## Subcommands

### `corrosion subs list`

Lists all subscriptions registered on this node.

```bash
corrosion subs list 
```

### `corrosion subs info`

View detailed informations on the subscription specified  `--hash <SUBSCRIPTION_HASH>` or `--id <UUID>`. If `--hash` is present it wins over `--id`. You must pass at least one of them; otherwise the command errors.

```bash
corrosion subs info --hash abc123... 
corrosion subs info --id 550e8400-e29b-41d4-a716-446655440000 
```
