package transformers

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/generators"
)

func TestMacAddress_Generate(t *testing.T) {
	type test struct {
		name               string
		original           []byte
		castType           int
		managementType     int
		keepOriginalVendor bool
	}

	tests := []test{
		{
			name:               "Random mac addr",
			original:           []byte("8c:7d:d9:b5:8b:d8"),
			keepOriginalVendor: false,
			managementType:     ManagementTypeAny,
			castType:           CastTypeAny,
		},
		{
			name:               "Random mac addr with keepOriginalVendor with Universal and Individual",
			original:           []byte("00:1a:2b:3c:4d:5e"),
			keepOriginalVendor: true,
			castType:           CastTypeAny,
			managementType:     ManagementTypeAny,
		},
		{
			name:               "Random mac addr with keepOriginalVendor with Universal and Group",
			original:           []byte("01:1a:2b:3c:4d:5e"),
			keepOriginalVendor: true,
			castType:           CastTypeAny,
			managementType:     ManagementTypeAny,
		},
		{
			name:               "Random mac addr with keepOriginalVendor with Local and Individual",
			original:           []byte("02:1a:2b:3c:4d:5e"),
			keepOriginalVendor: true,
			managementType:     ManagementTypeAny,
			castType:           CastTypeAny,
		},
		{
			name:               "Random mac addr with keepOriginalVendor with Local and Group",
			original:           []byte("03:1a:2b:3c:4d:5e"),
			keepOriginalVendor: true,
			managementType:     ManagementTypeAny,
			castType:           CastTypeAny,
		},
		{
			name:               "Random mac addr without keepOriginalVendor with Universal and Group",
			original:           []byte("03:1a:2b:3c:4d:5e"),
			keepOriginalVendor: false,
			managementType:     ManagementTypeUniversal,
			castType:           CastTypeGroup,
		},
		{
			name:               "Random mac addr without keepOriginalVendor with Universal and Individual",
			original:           []byte("03:1a:2b:3c:4d:5e"),
			keepOriginalVendor: false,
			managementType:     ManagementTypeUniversal,
			castType:           CastTypeIndividual,
		},
		{
			name:               "Random mac addr without keepOriginalVendor with Local and Individual",
			original:           []byte("03:1a:2b:3c:4d:5e"),
			keepOriginalVendor: false,
			managementType:     ManagementTypeLocal,
			castType:           CastTypeIndividual,
		},
		{
			name:               "Random mac addr without keepOriginalVendor with Universal and Individual",
			original:           []byte("03:1a:2b:3c:4d:5e"),
			keepOriginalVendor: false,
			managementType:     ManagementTypeLocal,
			castType:           CastTypeGroup,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := net.ParseMAC(string(tt.original))
			require.NoError(t, err)

			parsedMacOriginal, err := ExploreMacAddress(tt.original)
			require.NoError(t, err)

			tr, err := NewMacAddress()
			require.NoError(t, err)

			g := generators.NewRandomBytes(time.Now().UnixNano(), tr.GetRequiredGeneratorByteLength())
			//g, err := generators.NewHash(tt.original, "sha1")
			require.NoError(t, err)
			err = tr.SetGenerator(g)
			require.NoError(t, err)
			var res []byte
			res, err = tr.Generate(tt.original, tt.keepOriginalVendor, tt.castType, tt.managementType)
			require.NoError(t, err)

			parsedMacGenerated, err := ExploreMacAddress(res)
			require.NoError(t, err)

			if tt.keepOriginalVendor == true {
				require.True(
					t,
					parsedMacOriginal.CastType == parsedMacGenerated.CastType && parsedMacOriginal.ManagementType == parsedMacGenerated.ManagementType,
					fmt.Sprintf("Mac address info is't equals %+v != %+v", parsedMacOriginal, parsedMacGenerated),
				)
			}

			if tt.castType != CastTypeAny && tt.managementType != ManagementTypeAny {
				require.True(
					t,
					tt.castType == parsedMacGenerated.CastType && tt.managementType == parsedMacGenerated.ManagementType,
					fmt.Sprintf("Mac address info is't equals %+v != %+v", parsedMacOriginal, parsedMacGenerated),
				)
			}

			log.Debug().
				Str("macAddrOriginal", string(tt.original)).
				Str("macAddr", parsedMacGenerated.MacAddress.String()).
				Msg("result")
		})
	}
}
