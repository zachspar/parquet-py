{ pkgs ? import <nixpkgs> {} }:

let
  createVenv = ''
    if [ ! -d env ]; then
      python3 -m venv env
      source env/bin/activate
      pip install -U pip
      pip install 'maturin'
    else
      source env/bin/activate
    fi
  '';
in

pkgs.mkShell {
    nativeBuildInputs = with pkgs.buildPackages; [ python312 cmake ninja autoconf automake rustc cargo rustfmt libiconv ];
    shellHook = createVenv;
}
