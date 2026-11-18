{
  description = "mktpulse dev environment";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs =
    { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          python = pkgs.python313;
          nativeLibs = [ pkgs.stdenv.cc.cc.lib ];
        in
        {
          default = pkgs.mkShell {
            packages = [
              python
              pkgs.uv
              pkgs.git
              pkgs.jdk17
            ];

            shellHook = ''
              export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath nativeLibs}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
              export JAVA_HOME="${pkgs.jdk17.home}"
              export UV_PYTHON="${python}/bin/python3.13"
              export UV_PYTHON_DOWNLOADS=never
              export UV_PROJECT_ENVIRONMENT=".venv"
            '';
          };
        }
      );

      formatter = forAllSystems (system: nixpkgs.legacyPackages.${system}.nixfmt);
    };
}
