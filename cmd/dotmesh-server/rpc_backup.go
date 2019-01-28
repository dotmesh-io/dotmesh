package main

// func (d *DotmeshRPC) DumpEtcd(
// 	r *http.Request,
// 	args *struct {
// 		Prefix string
// 	},
// 	result *string,
// ) error {
// 	err := ensureAdminUser(r)
// 	if err != nil {
// 		return err
// 	}

// 	var backup types.BackupV1

// 	backup.Version = types.BackupVersion
// 	backup.Created = time.Now()

// 	users, err := d.usersManager.List("")
// 	if err != nil {
// 		log.WithFields(log.Fields{
// 			"error": err,
// 		}).Error("failed to list users")
// 	} else {
// 		backup.Users = users
// 	}

// 	filesystemMasters, err := d.state.filesystemStore.ListMaster()
// 	if err != nil {
// 		log.WithFields(log.Fields{
// 			"error": err,
// 		}).Error("failed to list filesystem masters")
// 	} else {
// 		backup.FilesystemMasters = filesystemMasters
// 	}

// 	registryFilesystems, err := d.state.registryStore.ListFilesystems()
// 	if err != nil {
// 		log.WithFields(log.Fields{
// 			"error": err,
// 		}).Error("failed to list registry filesystems")
// 	} else {
// 		backup.RegistryFilesystems = registryFilesystems
// 	}

// 	registryClones, err := d.state.registryStore.ListClones()
// 	if err != nil {
// 		log.WithFields(log.Fields{
// 			"error": err,
// 		}).Error("failed to list registry clones")
// 	} else {
// 		backup.RegistryClones = registryClones
// 	}

// 	resultBytes, err := json.Marshal(&backup)
// 	if err != nil {
// 		return err
// 	}

// 	*result = string(resultBytes)

// 	return nil
// }

// type restoreRequest struct {
// 	Prefix string
// 	Dump   string
// }

// // RestoreEtcd - restores KV store from the backup file
// func (d *DotmeshRPC) RestoreEtcd(r *http.Request, args *restoreRequest, result *bool) error {

// 	err := ensureAdminUser(r)
// 	if err != nil {
// 		return err
// 	}

// 	var backup types.BackupV1

// 	err = json.Unmarshal([]byte(args.Dump), &backup)
// 	if err != nil {
// 		return fmt.Errorf("failed to unmarshal into a backup structure: %s", err)
// 	}

// 	supported := false
// 	for _, v := range types.BackupSupportedVersions {
// 		if backup.Version == v {
// 			supported = true
// 		}
// 	}
// 	if !supported {
// 		return fmt.Errorf("unsupported backup version '%s', supported version: %s", backup.Version, strings.Join(types.BackupSupportedVersions, ", "))
// 	}

// 	var errs []error

// 	for _, u := range backup.Users {
// 		err = d.usersManager.Import(u)
// 		if err != nil {
// 			log.WithFields(log.Fields{
// 				"error": err,
// 				"user":  u.Name,
// 			}).Error("failed to import user")
// 			errs = append(errs, err)
// 		}
// 	}

// 	err = d.state.filesystemStore.ImportMasters(backup.FilesystemMasters, &store.ImportOptions{
// 		DeleteExisting: true,
// 	})
// 	if err != nil {
// 		errs = append(errs, err)
// 	}

// 	err = d.state.registryStore.ImportFilesystems(backup.RegistryFilesystems, &store.ImportOptions{
// 		DeleteExisting: true,
// 	})
// 	if err != nil {
// 		errs = append(errs, err)
// 	}

// 	err = d.state.registryStore.ImportClones(backup.RegistryClones, &store.ImportOptions{
// 		DeleteExisting: true,
// 	})
// 	if err != nil {
// 		errs = append(errs, err)
// 	}

// 	if len(errs) > 0 {
// 		return fmt.Errorf("got error while importing backup: %v", errs)
// 	}

// 	return nil
// }
