{ pkgs ? import <nixpkgs> { } }:

{
  inherit (pkgs)
    direnv
    neovim
    nodejs_20
    gh
    git
    yarn
    aws-sam-cli
    awscli2
    fzf
    bat
    ripgrep
    yazi
    file
;
}
