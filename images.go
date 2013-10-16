package gophercloud

import (
	"github.com/racker/perigee"
	"io"
	"os"
	"strings"
)

// See the CloudImagesProvider interface for details.
func (gsp *genericServersProvider) ListImages() ([]Image, error) {
	var is []Image

	err := gsp.context.WithReauth(gsp.access, func() error {
		var url string
		if strings.HasSuffix(gsp.endpoint, "v1") {
			url = gsp.endpoint + "/images/details"
		} else {
			url = gsp.endpoint + "/images"
		}
		return perigee.Get(url, perigee.Options{
			CustomClient: gsp.context.httpClient,
			Results:      &struct{ Images *[]Image }{&is},
			MoreHeaders: map[string]string{
				"X-Auth-Token": gsp.access.AuthToken(),
			},
		})
	})
	return is, err
}

func (gsp *genericServersProvider) ImageById(id string) (*Image, error) {
	var is *Image

	err := gsp.context.WithReauth(gsp.access, func() error {
		url := gsp.endpoint + "/images/" + id
		return perigee.Get(url, perigee.Options{
			CustomClient: gsp.context.httpClient,
			Results:      &struct{ Image **Image }{&is},
			MoreHeaders: map[string]string{
				"X-Auth-Token": gsp.access.AuthToken(),
			},
		})
	})
	return is, err
}

func (gsp *genericServersProvider) CreateNewImage(ni NewImage) (string, error) {
	response, err := gsp.context.ResponseWithReauth(gsp.access, func() (*perigee.Response, error) {
		url := gsp.endpoint + "/images"
		return perigee.Request("POST", url, perigee.Options{
			ReqBody: &struct {
				*NewImage `json:""`
			}{&ni},
			CustomClient: gsp.context.httpClient,
			MoreHeaders: map[string]string{
				"X-Auth-Token": gsp.access.AuthToken(),
			},
			OkCodes: []int{201},
		})
	})

	if err != nil {
		return "", err
	}

	location, err := response.HttpResponse.Location()
	if err != nil {
		return "", err
	}

	// Return the last element of the location which is the image id
	locationArr := strings.Split(location.Path, "/")
	return locationArr[len(locationArr)-1], err
}

// Stream a file as mime/multipart (application/octet-stream). The gist is
// to stream from a file, into (the write side of) a pipe, copy the file
// into the pipe, and then close the relevant file/pipe objects. This usually
// gets run asynchronously so HTTP requests can read from the read side
// of the pipe for the octet-stream. Any errors get set in ppError.
// This code was adapted from:
//    https://github.com/gebi/go-fileupload-example/blob/master/main.go
func streamFile(readFrom *os.File,
	readFromPath string,
	writePipe *io.PipeWriter,
	formLabel string,
	ppErr **error) {

	// Assure the file closes when exiting this function. Note that the
	// caller should not defer this close since this function likely runs
	// asynchronously.
	defer readFrom.Close()

	// Assure the write side of the pipe closes when exiting this function.
	defer writePipe.Close()

	// copy from the file to stream into the multipart.
	_, err := io.Copy(writePipe, readFrom)
	if err != nil {
		*ppErr = &err
		return
	}

	*ppErr = nil
}

func (gsp *genericServersProvider) UploadImageFile(imageId string,
	imagePath string) error {

	_, err := gsp.context.ResponseWithReauth(gsp.access,
		func() (*perigee.Response, error) {
			url := gsp.endpoint + "/images/" + imageId + "/file"

			// Get the file size for later http header setting.
			var fileSize int64
			fi, err := os.Stat(imagePath)
			if err != nil {
				return nil, err
			}
			fileSize = fi.Size()

			// Open the file to stream as multipart/octet-stream, but do not
			// defer its close here since it must remain open during the
			// streaming operation and the streamer will close it.
			inFile, err := os.Open(imagePath)
			if err != nil {
				return nil, err
			}

			// Create the body io.Reader (read side of pipe) and the writer
			// into which to write the application/octet-stream data.
			body, writer := io.Pipe()

			// Startup the streamer
			var streamErr *error
			go streamFile(inFile, imagePath, writer, "file", &streamErr)

			// Run the PUT request. The body will receive the octet-stream
			// from the streamer.
			return perigee.Request("PUT", url, perigee.Options{
				ReqBody:       body,
				CustomClient:  gsp.context.httpClient,
				ContentType:   "application/octet-stream",
				ContentLength: fileSize,
				MoreHeaders: map[string]string{
					"X-Auth-Token": gsp.access.AuthToken(),
				},
				OkCodes: []int{204},
			})
		})

	return err
}

func (gsp *genericServersProvider) DeleteImageById(id string) error {
	err := gsp.context.WithReauth(gsp.access, func() error {
		url := gsp.endpoint + "/images/" + id
		_, err := perigee.Request("DELETE", url, perigee.Options{
			CustomClient: gsp.context.httpClient,
			MoreHeaders: map[string]string{
				"X-Auth-Token": gsp.access.AuthToken(),
			},
		})
		return err
	})
	return err
}

// ImageLink provides a reference to a image by either ID or by direct URL.
// Some services use just the ID, others use just the URL.
// This structure provides a common means of expressing both in a single field.
type ImageLink struct {
	Id    string `json:"id"`
	Links []Link `json:"links"`
}

// Image is used for JSON (un)marshalling.
// It provides a description of an OS image.
//
// The Id field contains the image's unique identifier.
// For example, this identifier will be useful for specifying which operating system to install on a new server instance.
//
// The MinDisk and MinRam fields specify the minimum resources a server must provide to be able to install the image.
//
// The Name field provides a human-readable moniker for the OS image.
//
// The Progress and Status fields indicate image-creation status.
// Any usable image will have 100% progress.
//
// The Updated field indicates the last time this image was changed.
//
// OsDcfDiskConfig indicates the server's boot volume configuration.
// Valid values are:
//     AUTO
//     ----
//     The server is built with a single partition the size of the target flavor disk.
//     The file system is automatically adjusted to fit the entire partition.
//     This keeps things simple and automated.
//     AUTO is valid only for images and servers with a single partition that use the EXT3 file system.
//     This is the default setting for applicable Rackspace base images.
//
//     MANUAL
//     ------
//     The server is built using whatever partition scheme and file system is in the source image.
//     If the target flavor disk is larger,
//     the remaining disk space is left unpartitioned.
//     This enables images to have non-EXT3 file systems, multiple partitions, and so on,
//     and enables you to manage the disk configuration.
//
type Image struct {
	Created         string `json:"created"`
	Id              string `json:"id"`
	Links           []Link `json:"links"`
	MinDisk         int    `json:"minDisk"`
	MinRam          int    `json:"minRam"`
	Name            string `json:"name"`
	Progress        int    `json:"progress"`
	Status          string `json:"status"`
	Updated         string `json:"updated"`
	OsDcfDiskConfig string `json:"OS-DCF:diskConfig"`
}

// NewImage structures are used to create (upload) images.
// The fields discussed below are relevent for server-creation purposes.
//
// The Name field contains the desired name of the image.
// A name is required.
//
// The Visibility field contains visibility of the new image.
// If provided, this value must have either "public" or "private".
// This field is otional and defaults to "public".
//
// The Tags field contains key/value association of arbitrary data for tie image.
// This field defaults to an empty map if not provided.
//
// The Status field indicates the current status of the image
// This field is a return field only.
//
// The CreatedAt field indicates the time at which the image got created.
// This field is a return field only.
//
// The UpdatedAt field indicates the time at which the image got updated.
// This field is a return field only.
//
// The UpdatedAt field indicates the time at which the image got updated.
// This field is a return field only.
//
// The Self field indicates the URL to the image.
// This field is a return field only.
//
// The File field indicates the file URL to the image (e.g., for a GET).
// This field is a return field only.
//
// The Schema field indicates the schema of the server.
// This a return field only.
//
type NewImage struct {
	Name            string   `json:"name"`
	Visibility      string   `json:"visibility,omitempty"`
	ContainerFormat string   `json:"container_format"`
	DiskFormat      string   `json:"disk_format"`
	Tags            []string `json:"tags,omitempty"`
}
