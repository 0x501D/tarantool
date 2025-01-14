set(LUAROCKS_SOURCE_DIR "${PROJECT_SOURCE_DIR}/third_party/luarocks/src")
lua_source(lua_sources "${PROJECT_BINARY_DIR}/extra/luarocks/hardcoded.lua" luarocks_core_hardcoded_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/admin/cache.lua" luarocks_admin_cache_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/admin/cmd/add.lua" luarocks_admin_cmd_add_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/admin/cmd/make_manifest.lua" luarocks_admin_cmd_make_manifest_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/admin/cmd/refresh_cache.lua" luarocks_admin_cmd_refresh_cache_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/admin/cmd/remove.lua" luarocks_admin_cmd_remove_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/admin/index.lua" luarocks_admin_index_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/argparse.lua" luarocks_argparse_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/build.lua" luarocks_build_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/build/builtin.lua" luarocks_build_builtin_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/build/cmake.lua" luarocks_build_cmake_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/build/command.lua" luarocks_build_command_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/build/make.lua" luarocks_build_make_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd.lua" luarocks_cmd_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/build.lua" luarocks_cmd_build_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/config.lua" luarocks_cmd_config_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/doc.lua" luarocks_cmd_doc_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/download.lua" luarocks_cmd_download_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/init.lua" luarocks_cmd_init_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/install.lua" luarocks_cmd_install_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/lint.lua" luarocks_cmd_lint_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/list.lua" luarocks_cmd_list_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/make.lua" luarocks_cmd_make_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/new_version.lua" luarocks_cmd_new_version_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/pack.lua" luarocks_cmd_pack_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/path.lua" luarocks_cmd_path_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/purge.lua" luarocks_cmd_purge_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/remove.lua" luarocks_cmd_remove_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/search.lua" luarocks_cmd_search_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/show.lua" luarocks_cmd_show_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/test.lua" luarocks_cmd_test_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/unpack.lua" luarocks_cmd_unpack_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/upload.lua" luarocks_cmd_upload_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/which.lua" luarocks_cmd_which_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/cmd/write_rockspec.lua" luarocks_cmd_write_rockspec_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/core/cfg.lua" luarocks_core_cfg_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/core/dir.lua" luarocks_core_dir_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/core/manif.lua" luarocks_core_manif_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/core/path.lua" luarocks_core_path_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/core/persist.lua" luarocks_core_persist_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/core/sysdetect.lua" luarocks_core_sysdetect_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/core/util.lua" luarocks_core_util_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/core/vers.lua" luarocks_core_vers_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/deps.lua" luarocks_deps_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/deplocks.lua" luarocks_deplocks_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/dir.lua" luarocks_dir_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/download.lua" luarocks_download_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch.lua" luarocks_fetch_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/cvs.lua" luarocks_fetch_cvs_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/git.lua" luarocks_fetch_git_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/git_file.lua" luarocks_fetch_git_file_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/git_http.lua" luarocks_fetch_git_http_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/git_https.lua" luarocks_fetch_git_https_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/git_ssh.lua" luarocks_fetch_git_ssh_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/hg.lua" luarocks_fetch_hg_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/hg_http.lua" luarocks_fetch_hg_http_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/hg_https.lua" luarocks_fetch_hg_https_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/hg_ssh.lua" luarocks_fetch_hg_ssh_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/sscm.lua" luarocks_fetch_sscm_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fetch/svn.lua" luarocks_fetch_svn_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fs.lua" luarocks_fs_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fs/lua.lua" luarocks_fs_lua_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fs/tools.lua" luarocks_fs_tools_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fs/unix.lua" luarocks_fs_unix_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fs/unix/tools.lua" luarocks_fs_unix_tools_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/fun.lua" luarocks_fun_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/loader.lua" luarocks_loader_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/manif.lua" luarocks_manif_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/manif/writer.lua" luarocks_manif_writer_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/pack.lua" luarocks_pack_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/path.lua" luarocks_path_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/persist.lua" luarocks_persist_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/queries.lua" luarocks_queries_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/remove.lua" luarocks_remove_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/repos.lua" luarocks_repos_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/require.lua" luarocks_require_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/results.lua" luarocks_results_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/rockspecs.lua" luarocks_rockspecs_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/search.lua" luarocks_search_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/signing.lua" luarocks_signing_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/test.lua" luarocks_test_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/test/busted.lua" luarocks_test_busted_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/test/command.lua" luarocks_test_command_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/tools/patch.lua" luarocks_tools_patch_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/tools/tar.lua" luarocks_tools_tar_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/tools/zip.lua" luarocks_tools_zip_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/type/manifest.lua" luarocks_type_manifest_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/type/rockspec.lua" luarocks_type_rockspec_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/type_check.lua" luarocks_type_check_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/upload/api.lua" luarocks_upload_api_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/upload/multipart.lua" luarocks_upload_multipart_lua)
lua_source(lua_sources "${LUAROCKS_SOURCE_DIR}/luarocks/util.lua" luarocks_util_lua)
