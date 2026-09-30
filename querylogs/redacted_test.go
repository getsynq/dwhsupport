package querylogs

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestQueryLog_IsTextRedacted(t *testing.T) {
	tests := []struct {
		name string
		log  *QueryLog
		want bool
	}{
		{name: "nil log", log: nil},
		{name: "nil metadata", log: &QueryLog{}},
		{name: "key absent", log: &QueryLog{Metadata: NewMetadataStruct(map[string]*structpb.Value{"query_id": StringValue("q")})}},
		{
			name: "key false",
			log:  &QueryLog{Metadata: NewMetadataStruct(map[string]*structpb.Value{MetadataQueryTextRedacted: BoolValue(false)})},
		},
		{
			name: "key not a bool",
			log:  &QueryLog{Metadata: NewMetadataStruct(map[string]*structpb.Value{MetadataQueryTextRedacted: StringValue("true")})},
		},
		{
			name: "key true",
			log:  &QueryLog{Metadata: NewMetadataStruct(map[string]*structpb.Value{MetadataQueryTextRedacted: BoolValue(true)})},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.log.IsTextRedacted())
		})
	}
}
