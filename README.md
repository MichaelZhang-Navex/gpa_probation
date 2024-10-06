Welcome to your new dbt project!

## Initial environment setup

### Install mise

```bash
curl https://mise.run | sh
echo 'eval "$(~/.local/bin/mise activate bash)"' >> ~/.bashrc
echo 'eval "$(~/.local/bin/mise activate zsh)"' >> ~/.zshrc
```

### install everything

```bash
mise install
mise run
```

## Regular run

### Update data
```bash
# import / refresh raw excel data
make import_excel

# run everytime excel updated
make build
```

### Calculate data

```bash
# roll one student
make roll_one id=123456

# roll all
make roll_all
```
