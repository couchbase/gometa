//go:build community

package repository

func OpenMagmaRepositoryAndUpgrade(params RepoFactoryParams) (IRepository, error) {
	return nil, &StoreError{
		sType:     MagmaStoreType,
		storeCode: ErrNotSupported,
		errMsg:    "cannot open magma store in community edition",
	}
}
