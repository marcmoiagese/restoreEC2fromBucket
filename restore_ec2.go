package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Config struct {
	BucketName   string
	Prefix       string
	SubnetID     string
	IAMRole      string
	InstanceType string
	Region       string
	ProjectTag   string
	NameTag      string
}

func loadConfig(filePath string) (*Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error obrint restore.cfg: %v", err)
	}
	defer file.Close()

	config := &Config{}
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "bucketName":
			config.BucketName = value
		case "prefix":
			config.Prefix = value
		case "subnetID":
			config.SubnetID = value
		case "iamRole":
			config.IAMRole = value
		case "instanceType":
			config.InstanceType = value
		case "region":
			config.Region = value
		case "projectTag":
			config.ProjectTag = value
		case "nameTag":
			config.NameTag = value
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error llegint restore.cfg: %v", err)
	}

	// Validar camps obligatoris
	if config.BucketName == "" {
		return nil, fmt.Errorf("falta 'bucketName' al fitxer de configuració")
	}
	if config.SubnetID == "" {
		return nil, fmt.Errorf("falta 'subnetID' al fitxer de configuració")
	}
	if config.InstanceType == "" {
		return nil, fmt.Errorf("falta 'instanceType' al fitxer de configuració")
	}
	if config.Region == "" {
		return nil, fmt.Errorf("falta 'region' al fitxer de configuració")
	}

	return config, nil
}

// getCommonTags retorna les etiquetes comunes per a tots els recursos
func getCommonTags(config *Config) []types.Tag {
	return []types.Tag{
		{
			Key:   aws.String("Projekt"),
			Value: aws.String(config.ProjectTag),
		},
		{
			Key:   aws.String("Name"),
			Value: aws.String(config.NameTag),
		},
	}
}

// waitForVMDKsRestored espera que tots els fitxers VMDK estiguin disponibles al S3 (ja restaurats)
func waitForVMDKsRestored(ctx context.Context, client *s3.Client, config *Config, vmdkFiles []string) error {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	timeout := time.After(24 * time.Hour)

	for {
		select {
		case <-ticker.C:
			allRestored := true

			for _, key := range vmdkFiles {
				input := &s3.HeadObjectInput{
					Bucket: aws.String(config.BucketName),
					Key:    aws.String(key),
				}

				result, err := client.HeadObject(ctx, input)
				if err != nil {
					fmt.Printf("Error llegint capçalera de %s: %v", key, err)
					allRestored = false
					break
				}

				restoreHeader := ""
				if result.Restore != nil {
					restoreHeader = *result.Restore
				}

				if restoreHeader != "" && !strings.Contains(restoreHeader, "ongoing-request=\"false\"") {
					fmt.Printf("Restauració en curs per %s: %s", key, restoreHeader)
					allRestored = false
					break
				}
			}

			if allRestored {
				fmt.Printf("Tots els fitxers VMDK han estat restaurats del Glacier")
				return nil
			}

		case <-timeout:
			return fmt.Errorf("temps màxim d'espera excedit per restaurar fitxers")
		}
	}
}

func main() {
	ctx := context.Background()

	// Carregar configuració
	cfgPath := "restore.cfg"
	config, err := loadConfig(cfgPath)
	if err != nil {
		log.Fatalf("Error llegint configuració: %v", err)
	}

	// Configurar AWS
	awsCfg, err := awscfg.LoadDefaultConfig(ctx, awscfg.WithRegion(config.Region))
	if err != nil {
		log.Fatalf("Error carregant configuració AWS: %v", err)
	}

	s3Client := s3.NewFromConfig(awsCfg)
	ec2Client := ec2.NewFromConfig(awsCfg)

	// 1. Llistar tots els fitxers VMDK
	vmdkFiles, err := listVMDKFiles(ctx, s3Client, config)
	if err != nil {
		now := time.Now().Format("02-01-2006 15:04")
		log.Fatalf("[%s] Error llistant fitxers VMDK: %v", now, err)
	}
	now := time.Now().Format("02-01-2006 15:04")
	fmt.Printf("[%s] Trobats %d fitxers VMDK\n", now, len(vmdkFiles))

	// 2. Restaurar objectes de Glacier
	now = time.Now().Format("02-01-2006 15:04")
	fmt.Printf("[%s] Iniciant restauració des de Glacier...", now)
	for _, file := range vmdkFiles {
		err := restoreFromGlacier(ctx, s3Client, config, file)
		now = time.Now().Format("02-01-2006 15:04")
		if err != nil {
			fmt.Printf("[%s] Error restaurant %s: %v", now, file, err)
		} else {
			fmt.Printf("[%s] Restauració iniciada per %s\n", now, file)
		}
	}

	// 3. Esperar que tots els fitxers VMDK hagin acabat de restaurar
	now = time.Now().Format("02-01-2006 15:04")
	fmt.Printf("[%s] Esperant que tots els fitxers VMDK es restaurin del Glacier...", now)
	err = waitForVMDKsRestored(ctx, s3Client, config, vmdkFiles)
	if err != nil {
		log.Fatalf("Error esperant restauració de fitxers: %v", err)
	}

	// 4. Crear snapshots dels VMDK
	now = time.Now().Format("02-01-2006 15:04")
	fmt.Printf("[%s] Creant snapshots EBS...", now)
	snapshotIDs := make([]string, 0, len(vmdkFiles))
	for _, file := range vmdkFiles {
		snapshotID, err := createSnapshotFromVMDK(ctx, ec2Client, config, file)
		if err != nil {
			fmt.Printf("Error creant snapshot per %s: %v", file, err)
		} else {
			fmt.Printf("Snapshot creat: %s per %s\n", snapshotID, file)
			snapshotIDs = append(snapshotIDs, snapshotID)
		}
	}

	// 5. Esperar que els snapshots acabin
	now = time.Now().Format("02-01-2006 15:04")
	fmt.Printf("[%s] Esperant que snapshots estiguin completats...", now)

	type result struct {
		snapshotID string
		err        error
	}

	realSnapshotIDs := make([]string, 0, len(snapshotIDs))
	resultsChan := make(chan result, len(snapshotIDs))

	for _, importTaskID := range snapshotIDs {
		go func(taskID string) {
			realSnapshotID, err := waitForSnapshotComplete(ctx, ec2Client, taskID)
			resultsChan <- result{snapshotID: realSnapshotID, err: err}
		}(importTaskID)
	}

	for i := 0; i < len(snapshotIDs); i++ {
		res := <-resultsChan
		if res.err != nil {
			fmt.Printf("Error esperant snapshot: %v", res.err)
			continue
		}
		realSnapshotIDs = append(realSnapshotIDs, res.snapshotID)

		// Aplicar etiquetes immediatament
		_, err := ec2Client.CreateTags(ctx, &ec2.CreateTagsInput{
			Resources: []string{res.snapshotID},
			Tags:      getCommonTags(config),
		})
		if err != nil {
			fmt.Printf("Error afegint etiquetes a la snapshot %s: %v", res.snapshotID, err)
		} else {
			fmt.Printf("Etiquetes aplicades a la snapshot %s\n", res.snapshotID)
		}
	}

	if len(realSnapshotIDs) == 0 {
		log.Fatalf("No s'han completat cap snapshot")
	}
	snapshotIDs = realSnapshotIDs

	// Validacio manual dels snapshots
	//aws ec2 describe-snapshots --snapshot-ids < Ids > --query 'Snapshots[*].[SnapshotId,Tags]'

	// 6. Crear volums EBS a partir dels snapshots
	now = time.Now().Format("02-01-2006 15:04")
	fmt.Printf("[%s] Creant volums EBS...", now)
	volumeIDs := make([]string, 0, len(snapshotIDs))
	for _, snapshotID := range snapshotIDs {
		volumeID, err := createVolumeFromSnapshot(ctx, ec2Client, config, snapshotID)
		if err != nil {
			fmt.Printf("Error creant volum per snapshot %s: %v", snapshotID, err)
		} else {
			fmt.Printf("Volum creat: %s\n", volumeID)
			volumeIDs = append(volumeIDs, volumeID)

			_, err = ec2Client.CreateTags(ctx, &ec2.CreateTagsInput{
				Resources: []string{volumeID},
				Tags:      getCommonTags(config),
			})
			if err != nil {
				fmt.Printf("Error afegint etiquetes al volum %s: %v", volumeID, err)
			}
		}
	}

	// 7. Crear AMI a partir del primer snapshot (root)
	now = time.Now().Format("02-01-2006 15:04")
	fmt.Printf("[%s] Creant AMI a partir del primer snapshot...", now)
	rootSnapshotID := snapshotIDs[0]
	amiID, err := createAMIFromRootSnapshot(ctx, ec2Client, config, rootSnapshotID)
	if err != nil {
		log.Fatalf("Error registrant AMI: %v", err)
	}

	// 7.1 Afegir etiquetes a l'AMI
	now = time.Now().Format("02-01-2006 15:04")
	fmt.Printf("[%s] Afegint etiquetes a la AMI...", now)
	_, err = ec2Client.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{amiID},
		Tags:      getCommonTags(config),
	})
	if err != nil {
		fmt.Printf("Error afegint etiquetes a l'AMI %s: %v", amiID, err)
	}

	// 8. Crear instància amb tots els volums adjuntats directament
	now = time.Now().Format("02-01-2006 15:04")
	fmt.Printf("[%s] Creant instancia amb tots els volums...", now)
	instanceID, err := createEC2InstanceWithVolumes(ctx, ec2Client, config, amiID, volumeIDs, vmdkFiles)
	if err != nil {
		log.Fatalf("Error creant instància EC2: %v", err)
	}

	// 9. Afegir etiquetes a la instància
	now = time.Now().Format("02-01-2006 15:04")
	fmt.Printf("[%s] Afegint etiquetes a l'instancia...", now)
	_, err = ec2Client.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{instanceID},
		Tags:      getCommonTags(config),
	})
	if err != nil {
		fmt.Printf("Error afegint etiquetes a la instància %s: %v", instanceID, err)
	}
	fmt.Printf("Instància EC2 creada amb èxit: %s\n", instanceID)

	// 10. tornar els fitxers VMDK al Glacier
	/*fmt.Printf("Retornant fitxers VMDK al Glacier...")
	for _, file := range vmdkFiles {
		err := moveVMDKBackToGlacier(ctx, s3Client, config, file)
		if err != nil {
			fmt.Printf("Error retornant %s al Glacier: %v", file, err)
		} else {
			fmt.Printf("Fitxer %s retornat al Glacier\n", file)
		}
	}*/
}

// Llistar fitxers .vmdk al bucket
func listVMDKFiles(ctx context.Context, client *s3.Client, config *Config) ([]string, error) {
	var files []string
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(config.BucketName),
		Prefix: aws.String(config.Prefix),
	}
	paginator := s3.NewListObjectsV2Paginator(client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, ".vmdk") {
				files = append(files, *obj.Key)
			}
		}
	}
	return files, nil
}

// Restaurar fitxer de Glacier
func restoreFromGlacier(ctx context.Context, client *s3.Client, config *Config, key string) error {
	input := &s3.RestoreObjectInput{
		Bucket: aws.String(config.BucketName),
		Key:    aws.String(key),
		RestoreRequest: &s3types.RestoreRequest{
			Days: aws.Int32(7),
			GlacierJobParameters: &s3types.GlacierJobParameters{
				Tier: s3types.TierBulk,
			},
		},
	}
	_, err := client.RestoreObject(ctx, input)
	return err
}

// Crear snapshot a partir del VMDK
func createSnapshotFromVMDK(ctx context.Context, client *ec2.Client, config *Config, vmdkKey string) (string, error) {
	input := &ec2.ImportSnapshotInput{
		Description: aws.String(fmt.Sprintf("Restored from %s", vmdkKey)),
		DiskContainer: &types.SnapshotDiskContainer{
			Format: aws.String("vmdk"),
			UserBucket: &types.UserBucket{
				S3Bucket: aws.String(config.BucketName),
				S3Key:    aws.String(vmdkKey),
			},
		},
	}
	result, err := client.ImportSnapshot(ctx, input)
	if err != nil {
		return "", err
	}
	return *result.ImportTaskId, nil
}

// Esperar que el snapshot acabi
func waitForSnapshotComplete(ctx context.Context, client *ec2.Client, importTaskID string) (string, error) {
	for {
		input := &ec2.DescribeImportSnapshotTasksInput{
			ImportTaskIds: []string{importTaskID},
		}
		result, err := client.DescribeImportSnapshotTasks(ctx, input)
		if err != nil {
			return "", fmt.Errorf("error descrivint tasca d'importació (%s): %v", importTaskID, err)
		}
		if len(result.ImportSnapshotTasks) == 0 {
			return "", fmt.Errorf("no s'ha trobat la tasca d'importació: %s", importTaskID)
		}
		task := result.ImportSnapshotTasks[0]

		var status string
		if task.SnapshotTaskDetail != nil && task.SnapshotTaskDetail.Status != nil {
			status = *task.SnapshotTaskDetail.Status
		} else {
			status = "unknown"
		}

		if task.SnapshotTaskDetail != nil {
			if status == "completed" {
				if task.SnapshotTaskDetail.SnapshotId != nil {
					snapshotID := *task.SnapshotTaskDetail.SnapshotId
					fmt.Printf("[TASK %s] Snapshot completada amb ID: %s\n", importTaskID, snapshotID)
					return snapshotID, nil
				}
				return "", fmt.Errorf("snapshot completada però sense ID")
			} else if status == "error" {
				msg := "desconegut"
				if task.SnapshotTaskDetail.StatusMessage != nil {
					msg = *task.SnapshotTaskDetail.StatusMessage
				}
				return "", fmt.Errorf("[TASK %s] error en la tasca d'importació: %s", importTaskID, msg)
			}
		}
		fmt.Printf("[TASK %s] Esperant que la snapshot finalitzi... Estat actual: %s\n", importTaskID, status)
		time.Sleep(30 * time.Second)
	}
}

// Crear volum EBS a partir de snapshot
func createVolumeFromSnapshot(ctx context.Context, client *ec2.Client, config *Config, snapshotID string) (string, error) {
	subnetInput := &ec2.DescribeSubnetsInput{
		SubnetIds: []string{config.SubnetID},
	}
	subnetResult, err := client.DescribeSubnets(ctx, subnetInput)
	if err != nil {
		return "", err
	}
	az := *subnetResult.Subnets[0].AvailabilityZone

	input := &ec2.CreateVolumeInput{
		AvailabilityZone: aws.String(az),
		SnapshotId:       aws.String(snapshotID),
		VolumeType:       types.VolumeTypeGp3,
		Iops:             aws.Int32(3000),
		Throughput:       aws.Int32(125),
		TagSpecifications: []types.TagSpecification{
			{
				ResourceType: types.ResourceTypeVolume,
				Tags:         getCommonTags(config),
			},
		},
	}
	result, err := client.CreateVolume(ctx, input)
	if err != nil {
		return "", err
	}
	return *result.VolumeId, nil
}

// Crear AMI a partir del snapshot root
func createAMIFromRootSnapshot(ctx context.Context, client *ec2.Client, config *Config, snapshotID string) (string, error) {
	input := &ec2.RegisterImageInput{
		Name:               aws.String("CustomAMI-Restored"),
		Description:        aws.String("AMI restaurada a partir del disc root"),
		RootDeviceName:     aws.String("/dev/sda1"),
		VirtualizationType: aws.String("hvm"),
		EnaSupport:         aws.Bool(true),
		Architecture:       types.ArchitectureValuesX8664,
		BlockDeviceMappings: []types.BlockDeviceMapping{
			{
				DeviceName: aws.String("/dev/sda1"),
				Ebs: &types.EbsBlockDevice{
					SnapshotId: aws.String(snapshotID),
					VolumeType: types.VolumeTypeGp3,
					Iops:       aws.Int32(3000),
					Throughput: aws.Int32(125),
				},
			},
		},
	}
	result, err := client.RegisterImage(ctx, input)
	if err != nil {
		return "", err
	}
	return *result.ImageId, nil
}

// Extreure nom del dispositiu a partir del fitxer VMDK
func extractDeviceNameFromKey(key string) string {
	parts := strings.Split(key, "-dev-")
	if len(parts) < 2 {
		return ""
	}
	devPart := strings.TrimSuffix(parts[1], ".vmdk")
	return "/dev/" + devPart
}

// Mapejar volums a noms de dispositius
func mapVolumeIDsToDeviceNames(volumeIDs []string, vmdkFiles []string) map[string]string {
	mapping := make(map[string]string)
	for i, volID := range volumeIDs {
		if i < len(vmdkFiles) {
			devName := extractDeviceNameFromKey(vmdkFiles[i])
			if devName != "" {
				mapping[volID] = devName
			}
		}
	}
	return mapping
}

// Crear instància amb tots els volums adjuntats
func createEC2InstanceWithVolumes(ctx context.Context, client *ec2.Client, config *Config, amiID string, volumeIDs []string, vmdkFiles []string) (string, error) {
	runInput := &ec2.RunInstancesInput{
		ImageId:          aws.String(amiID),
		InstanceType:     types.InstanceType(config.InstanceType),
		MinCount:         aws.Int32(1),
		MaxCount:         aws.Int32(1),
		SubnetId:         aws.String(config.SubnetID),
		SecurityGroupIds: []string{"sg-05c55aea4c7177179"},
		IamInstanceProfile: &types.IamInstanceProfileSpecification{
			Name: aws.String(config.IAMRole),
		},
		TagSpecifications: []types.TagSpecification{
			{
				ResourceType: types.ResourceTypeInstance,
				Tags:         getCommonTags(config),
			},
		},
	}

	runResult, err := client.RunInstances(ctx, runInput)
	if err != nil {
		return "", err
	}
	instanceID := *runResult.Instances[0].InstanceId

	// Esperar que l'estat sigui "running"
	err = waitForInstanceRunning(ctx, client, instanceID)
	if err != nil {
		return "", err
	}

	// Aturar la instància per adjuntar volums
	_, err = client.StopInstances(ctx, &ec2.StopInstancesInput{
		InstanceIds: []string{instanceID},
	})
	if err != nil {
		return "", err
	}
	err = waitForInstanceStopped(ctx, client, instanceID)
	if err != nil {
		return "", err
	}

	// Desvincular el volum temporal root
	descInput := &ec2.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	}
	descResult, err := client.DescribeInstances(ctx, descInput)
	if err != nil {
		return "", err
	}
	instance := descResult.Reservations[0].Instances[0]
	for _, bdm := range instance.BlockDeviceMappings {
		if *bdm.DeviceName == *instance.RootDeviceName {
			_, err = client.DetachVolume(ctx, &ec2.DetachVolumeInput{
				VolumeId: bdm.Ebs.VolumeId,
			})
			if err != nil {
				return "", err
			}
			break
		}
	}

	// Esperem uns segons per assegurar-nos que el volum estigui desconnectat
	log.Println("Esperant 60 segons perquè el volum temporal es desconnecti...")
	time.Sleep(60 * time.Second)

	// Obtenir mapeig entre volums i noms de dispositius
	deviceMapping := mapVolumeIDsToDeviceNames(volumeIDs, vmdkFiles)

	// Adjuntar cada volum
	for volID, devName := range deviceMapping {
		_, err := client.AttachVolume(ctx, &ec2.AttachVolumeInput{
			VolumeId:   aws.String(volID),
			InstanceId: aws.String(instanceID),
			Device:     aws.String(devName),
		})
		if err != nil {
			return "", err
		}
		fmt.Printf("Volum %s adjuntat a %s\n", volID, devName)
	}

	// Reiniciar la instància
	_, err = client.StartInstances(ctx, &ec2.StartInstancesInput{
		InstanceIds: []string{instanceID},
	})
	if err != nil {
		return "", err
	}

	fmt.Printf("Instància EC2 creada amb tots els volums: %s\n", instanceID)
	return instanceID, nil
}

// Esperar que la instància s'aturi
func waitForInstanceStopped(ctx context.Context, client *ec2.Client, instanceID string) error {
	for {
		input := &ec2.DescribeInstancesInput{
			InstanceIds: []string{instanceID},
		}
		result, err := client.DescribeInstances(ctx, input)
		if err != nil {
			return err
		}
		state := result.Reservations[0].Instances[0].State.Name
		if state == types.InstanceStateNameStopped {
			return nil
		} else if state == types.InstanceStateNameTerminated {
			return fmt.Errorf("la instància ha estat terminada")
		}
		fmt.Printf("Esperant que la instància %s s'aturi... Estat actual: %s\n", instanceID, state)
		time.Sleep(10 * time.Second)
	}
}

// Esperar que la instància estigui running
func waitForInstanceRunning(ctx context.Context, client *ec2.Client, instanceID string) error {
	for {
		input := &ec2.DescribeInstancesInput{
			InstanceIds: []string{instanceID},
		}
		result, err := client.DescribeInstances(ctx, input)
		if err != nil {
			return err
		}
		state := result.Reservations[0].Instances[0].State.Name
		if state == types.InstanceStateNameRunning {
			return nil
		} else if state == types.InstanceStateNameTerminated || state == types.InstanceStateNameShuttingDown {
			return fmt.Errorf("la instància està en estat %s", state)
		}
		time.Sleep(10 * time.Second)
	}
}

// Tornar a posar el fitxer al Glacier
func moveVMDKBackToGlacier(ctx context.Context, client *s3.Client, config *Config, key string) error {
	input := &s3.CopyObjectInput{
		Bucket:            aws.String(config.BucketName),
		CopySource:        aws.String(config.BucketName + "/" + key),
		Key:               aws.String(key),
		StorageClass:      s3types.StorageClassGlacier,
		MetadataDirective: s3types.MetadataDirectiveReplace,
	}
	_, err := client.CopyObject(ctx, input)
	return err
}
